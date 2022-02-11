package bow

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/marshal"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

var mapParquetToBowTypes = map[parquet.Type]Type{
	parquet.Type_BOOLEAN:    Boolean,
	parquet.Type_INT64:      Int64,
	parquet.Type_DOUBLE:     Float64,
	parquet.Type_BYTE_ARRAY: String,
}

var mapBowToParquetTypes = map[Type]parquet.Type{
	Boolean: parquet.Type_BOOLEAN,
	Int64:   parquet.Type_INT64,
	Float64: parquet.Type_DOUBLE,
	String:  parquet.Type_BYTE_ARRAY,
}

const keyParquetColTypesMeta = "col_types"

type parquetColTypesMeta struct {
	Name        string               `json:"name"`
	LogicalType *parquet.LogicalType `json:"logical_type"`
}

// NewBowFromParquet loads a parquet object from the file path, returning a new Bow
// Only value columns are used to create the new Bow.
// Argument verbose is used to print information about the file loaded.
func NewBowFromParquet(path string, verbose bool) (Bow, error) {
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		return nil, fmt.Errorf("local.NewLocalFileReader: %w", err)
	}

	pr := new(reader.ParquetReader)
	pr.NP = 4
	pr.PFile = fr
	if err = pr.ReadFooter(); err != nil {
		return nil, fmt.Errorf("reader.ParquetReader.ReadFooter: %w", err)
	}
	pr.ColumnBuffers = make(map[string]*reader.ColumnBufferType)
	pr.SchemaHandler = schema.NewSchemaHandlerFromSchemaList(pr.Footer.GetSchema())

	var originalColNames = make([]string, len(pr.Footer.GetSchema()))
	for i, se := range pr.Footer.GetSchema() {
		originalColNames[i] = se.Name
	}

	var originalRowGroups = make([]*parquet.RowGroup, len(pr.Footer.RowGroups))
	for r, rg := range pr.Footer.RowGroups {
		var originalCols = make([]*parquet.ColumnChunk, len(rg.Columns))
		for c, col := range rg.Columns {
			var originalMetaData = parquet.ColumnMetaData{
				PathInSchema: col.MetaData.PathInSchema,
			}
			var originalCol = parquet.ColumnChunk{
				MetaData: &originalMetaData,
			}
			originalCols[c] = &originalCol
		}
		originalRowGroups[r] = &parquet.RowGroup{Columns: originalCols}
	}

	pr.RenameSchema()

	var valueColIndex int64
	var series = make([]Series, pr.SchemaHandler.GetColumnNum())
	var parquetColTypesMetas []parquetColTypesMeta
	for colIndex, col := range pr.Footer.GetSchema() {
		if col.NumChildren != nil {
			continue
		}

		if col.ConvertedType != nil || col.LogicalType != nil {
			parquetColTypesMetas = append(parquetColTypesMetas, parquetColTypesMeta{
				Name:        originalColNames[colIndex],
				LogicalType: col.LogicalType,
			})
		}

		values, _, _, err := pr.ReadColumnByIndex(valueColIndex, pr.GetNumRows())
		if err != nil {
			return nil, fmt.Errorf("reader.ParquetReader.ReadColumnByIndex: %w", err)
		}

		bowType := mapParquetToBowTypes[col.GetType()]
		buf := NewBuffer(len(values), bowType)
		for i, v := range values {
			buf.SetOrDrop(i, v)
		}
		series[valueColIndex] = NewSeriesFromBuffer(originalColNames[colIndex], buf)

		pr.Footer.Schema[colIndex].Name = originalColNames[colIndex]
		valueColIndex++
	}

	for r, rg := range pr.Footer.RowGroups {
		for c := range rg.Columns {
			pr.Footer.RowGroups[r].Columns[c].MetaData.PathInSchema = originalRowGroups[r].
				Columns[c].MetaData.PathInSchema
		}
	}

	var keys, values []string
	for _, m := range pr.Footer.KeyValueMetadata {
		if m.GetKey() != "ARROW:schema" && m.GetKey() != keyParquetColTypesMeta {
			keys = append(keys, m.GetKey())
			values = append(values, m.GetValue())
		}
	}

	if len(parquetColTypesMetas) > 0 {
		colTypesJSON, err := json.Marshal(parquetColTypesMetas)
		if err != nil {
			return nil, fmt.Errorf("json.Marshal: %w", err)
		}
		keys = append(keys, keyParquetColTypesMeta)
		values = append(values, string(colTypesJSON))
	}

	b, err := NewBowWithMetadata(NewMetadata(keys, values), series...)
	if err != nil {
		return nil, fmt.Errorf("NewBowWithMetadata: %w", err)
	}

	footerIndented, err := json.MarshalIndent(pr.Footer, "", "\t")
	if err != nil {
		return nil, fmt.Errorf("json.MarshalIndent: %w", err)
	}

	if verbose {
		fmt.Printf(
			"bow.NewBowFromParquet: %s successfully read: %d rows\n%+v\n%+v\n",
			path, b.NumRows(), b.Schema().String(), string(footerIndented))
	}

	return b, nil
}

// WriteParquet writes a Bow to the binary parquet format.
// Argument verbose is used to print information about the written file.
func (b *bow) WriteParquet(path string, verbose bool) error {
	if b.NumCols() == 0 {
		return fmt.Errorf("bow has 0 columns")
	}

	if !strings.HasSuffix(path, ".parquet") {
		path += ".parquet"
	}

	var parquetColTypesMetas []parquetColTypesMeta
	keyIndex := b.Metadata().FindKey(keyParquetColTypesMeta)
	if keyIndex != -1 {
		var err error
		parquetColTypesMetas, err = readColTypesMeta(b, b.Metadata().Values()[keyIndex])
		if err != nil {
			return fmt.Errorf("readColTypesMeta: %w", err)
		}
	}

	sElem := parquet.NewSchemaElement()
	requiredRepType := parquet.FieldRepetitionType_REQUIRED
	sElem.RepetitionType = &requiredRepType
	sElem.Name = "Schema"
	numChildren := int32(b.NumCols())
	sElem.NumChildren = &numChildren

	var sElems []*parquet.SchemaElement
	sElems = append(sElems, sElem)
	lTypes := []*parquet.LogicalType{nil}
	for i, f := range b.Schema().Fields() {
		parquetType := mapBowToParquetTypes[b.ColumnType(i)]
		sElem = parquet.NewSchemaElement()
		sElem.Type = &parquetType
		optionalRepType := parquet.FieldRepetitionType_OPTIONAL
		sElem.RepetitionType = &optionalRepType
		sElem.Name = f.Name
		for j, t := range parquetColTypesMetas {
			if t.Name == f.Name {
				sElem.LogicalType = parquetColTypesMetas[j].LogicalType
			}
		}
		sElems = append(sElems, sElem)
		lTypes = append(lTypes, sElem.LogicalType)
	}

	parquetFile, err := local.NewLocalFileWriter(path)
	if err != nil {
		return fmt.Errorf("local.NewLocalFileWriter: %w", err)
	}
	defer parquetFile.Close()

	parquetWriter, err := newJSONWriter(sElems, parquetFile, 4)
	if err != nil {
		return fmt.Errorf("newJSONWriter: %w", err)
	}

	for k, key := range b.Metadata().Keys() {
		if key != "ARROW:schema" {
			parquetWriter.Footer.KeyValueMetadata = append(parquetWriter.Footer.KeyValueMetadata,
				&parquet.KeyValue{
					Key:   key,
					Value: &b.Metadata().Values()[k],
				})
		}
	}

	for i, lt := range lTypes {
		parquetWriter.SchemaHandler.SchemaElements[i].LogicalType = lt
	}

	for row := range b.GetRowsChan() {
		rowJSON, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("json.Marshal: %w", err)
		}
		if err = parquetWriter.Write(string(rowJSON)); err != nil {
			return fmt.Errorf("JSONWriter.Write: %w", err)
		}
	}

	if err = parquetWriter.WriteStop(); err != nil {
		return fmt.Errorf("JSONWriter.WriteStop: %w", err)
	}

	footerBytes, err := json.MarshalIndent(parquetWriter.Footer, "", "\t")
	if err != nil {
		return fmt.Errorf("json.MarshalIndent: %w", err)
	}

	if verbose {
		fmt.Printf(
			"bow.WriteParquet: %s successfully written: %d rows\n%s\n",
			path, parquetWriter.Footer.NumRows, string(footerBytes))
	}

	return nil
}

var ErrColTimeUnitNotFound = errors.New("column time unit not found in parquet metadata")

// GetParquetMetaColTimeUnit attempts to get the time unit of the column as a time.Duration
// from the bow metadata read from a parquet file.
// If no time unit metadata is found, time.Duration(0) is returned along with ErrColTimeUnitNotFound.
func (b *bow) GetParquetMetaColTimeUnit(colIndex int) (time.Duration, error) {
	colName := b.ColumnName(colIndex)

	keyIndex := b.Metadata().FindKey(keyParquetColTypesMeta)
	if keyIndex == -1 {
		return time.Duration(0), ErrColTimeUnitNotFound
	}

	colTypesMeta, err := readColTypesMeta(b, b.Metadata().Values()[keyIndex])
	if err != nil {
		return time.Duration(0), fmt.Errorf("readColTypesMeta: %w", err)
	}

	for _, m := range colTypesMeta {
		if m.Name == colName {
			if m.LogicalType != nil &&
				m.LogicalType.IsSetTIMESTAMP() {
				unit := m.LogicalType.TIMESTAMP.GetUnit()
				switch {
				case unit.IsSetMILLIS():
					return time.Millisecond, nil
				case unit.IsSetMICROS():
					return time.Microsecond, nil
				case unit.IsSetNANOS():
					return time.Nanosecond, nil
				}
			}
		}
	}

	return time.Duration(0), ErrColTimeUnitNotFound
}

func readColTypesMeta(b Bow, jsonEncodedData string) ([]parquetColTypesMeta, error) {
	var colTypesMeta []parquetColTypesMeta
	if err := json.Unmarshal([]byte(jsonEncodedData), &colTypesMeta); err != nil {
		return nil, fmt.Errorf("invalid column types metadata: %+v", jsonEncodedData)
	}

	if len(colTypesMeta) > b.NumCols() {
		return nil, fmt.Errorf("invalid column types metadata: %+v", colTypesMeta)
	}

	var countByCols = make([]int, b.NumCols())
	for _, t := range colTypesMeta {
		colFound := false
		for i, f := range b.Schema().Fields() {
			if t.Name == f.Name {
				countByCols[i]++
				colFound = true
			}
		}
		if !colFound {
			return nil, fmt.Errorf("invalid column types metadata: %+v", colTypesMeta)
		}
	}

	for _, count := range countByCols {
		if count > 1 {
			return nil, fmt.Errorf("invalid column types metadata: %+v", colTypesMeta)
		}
	}

	return colTypesMeta, nil
}

func newJSONWriter(se []*parquet.SchemaElement, pfile source.ParquetFile, np int64) (*writer.JSONWriter, error) {
	res := new(writer.JSONWriter)
	res.SchemaHandler = schema.NewSchemaHandlerFromSchemaList(se)
	res.SchemaHandler.CreateInExMap()

	res.PFile = pfile
	res.PageSize = 8 * 1024              //8K
	res.RowGroupSize = 128 * 1024 * 1024 //128M
	res.CompressionType = parquet.CompressionCodec_SNAPPY
	res.PagesMapBuf = make(map[string][]*layout.Page)
	res.DictRecs = make(map[string]*layout.DictRecType)
	res.NP = np
	res.Footer = parquet.NewFileMetaData()
	res.Footer.Version = 1
	res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
	res.Offset = 4
	res.MarshalFunc = marshal.MarshalJSON

	_, err := res.PFile.Write([]byte("PAR1"))
	if err != nil {
		return nil, err
	}

	return res, nil
}
