package bow

import (
	"encoding/json"
	"fmt"
	"github.com/xitongsys/parquet-go/tool/parquet-tools/schematool"
	"github.com/xitongsys/parquet-go/writer"
	"strings"

	"github.com/apache/arrow/go/arrow"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/schema"
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

const keyParquetMetaColTypes = "col_types"

type parquetColTypesMeta struct {
	Name          string                 `json:"name"`
	ConvertedType *parquet.ConvertedType `json:"converted_type"`
	LogicalType   *parquet.LogicalType   `json:"logical_type"`
}

// NewBowFromParquet loads a parquet object from the file path, returning a new Bow
// Only value columns are used to create the new Bow.
// Argument verbose is used to print information about the file loaded.
func NewBowFromParquet(path string, verbose bool) (Bow, error) {
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		return nil, fmt.Errorf("bow.NewBowFromParquet: %w", err)
	}

	pr := new(reader.ParquetReader)
	pr.NP = 4
	pr.PFile = fr
	if err = pr.ReadFooter(); err != nil {
		return nil, fmt.Errorf("bow.NewBowFromParquet: %w", err)
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
				Name:          originalColNames[colIndex],
				ConvertedType: col.ConvertedType,
				LogicalType:   col.LogicalType,
			})
		}

		values, _, _, err := pr.ReadColumnByIndex(valueColIndex, pr.GetNumRows())
		if err != nil {
			return nil, fmt.Errorf("bow.NewBowFromParquet: %w", err)
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
		if m.GetKey() != "ARROW:schema" && m.GetKey() != keyParquetMetaColTypes {
			keys = append(keys, m.GetKey())
			values = append(values, m.GetValue())
		}
	}

	if len(parquetColTypesMetas) > 0 {
		bytes, err := json.Marshal(parquetColTypesMetas)
		if err != nil {
			panic(fmt.Errorf("bow.NewBowFromParquet: %w", err))
		}
		keys = append(keys, keyParquetMetaColTypes)
		values = append(values, string(bytes))
	}

	b, err := NewBowWithMetadata(NewMetadata(keys, values), series...)
	if err != nil {
		return nil, fmt.Errorf("bow.NewBowFromParquet: %w", err)
	}

	footerIndented, err := json.MarshalIndent(pr.Footer, "", "\t")
	if err != nil {
		return nil, fmt.Errorf("bow.NewBowFromParquet: %w", err)
	}

	if verbose {
		fmt.Printf(
			"bow.NewBowFromParquet: %s successfully read: %d rows\n%+v\n%+v\n",
			path, b.NumRows(), b.Schema().String(), string(footerIndented))
	}

	return b, nil
}

// WriteParquet writes a Bow to the binary parquet format.
// Argument verbose is used to print information about the file written.
func (b *bow) WriteParquet(path string, verbose bool) error {
	if b.NumCols() == 0 {
		return fmt.Errorf("bow.WriteParquet: no columns")
	}

	if !strings.HasSuffix(path, ".parquet") {
		path += ".parquet"
	}

	var parquetMeta []*parquet.KeyValue
	bowMeta := b.Metadata()
	bowMetaValues := bowMeta.Values()

	var parquetColTypesMetas []parquetColTypesMeta
	for k, key := range bowMeta.Keys() {
		if key != "ARROW:schema" {
			parquetMeta = append(parquetMeta, &parquet.KeyValue{
				Key:   key,
				Value: &bowMetaValues[k],
			})
		}

		if key == keyParquetMetaColTypes {
			var err error
			parquetColTypesMetas, err = validateColTypesMeta(b, bowMetaValues[k])
			if err != nil {
				return fmt.Errorf("bow.WriteParquet: %w", err)
			}
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
				sElem.ConvertedType = parquetColTypesMetas[j].ConvertedType
				sElem.LogicalType = parquetColTypesMetas[j].LogicalType
			}
		}
		sElems = append(sElems, sElem)
		lTypes = append(lTypes, sElem.LogicalType)
	}

	parquetFile, err := local.NewLocalFileWriter(path)
	if err != nil {
		return fmt.Errorf("bow.WriteParquet: local.NewLocalFileWriter: %w", err)
	}
	defer parquetFile.Close()

	sTree := schematool.CreateSchemaTree(sElems)
	parquetWriter, err := writer.NewJSONWriter(sTree.OutputJsonSchema(), parquetFile, 4)
	if err != nil {
		return fmt.Errorf("bow.WriteParquet: newJSONWriter: %w", err)
	}

	parquetWriter.Footer.KeyValueMetadata = parquetMeta

	for i, lt := range lTypes {
		parquetWriter.SchemaHandler.SchemaElements[i].LogicalType = lt
	}

	for row := range b.GetRowsChan() {
		rowJSON, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("bow.WriteParquet: json.Marshal: %w", err)
		}
		if err = parquetWriter.Write(string(rowJSON)); err != nil {
			return fmt.Errorf("bow.WriteParquet: JSONWriter.Write: %w", err)
		}
	}

	err = parquetWriter.WriteStop()
	if err != nil {
		return fmt.Errorf("bow.WriteParquet: JSONWriter.WriteStop: %w", err)
	}

	footerBytes, err := json.MarshalIndent(parquetWriter.Footer, "", "\t")
	if err != nil {
		return fmt.Errorf("bow.WriteParquet: json.MarshalIndent: %w", err)
	}

	if verbose {
		fmt.Printf(
			"bow.WriteParquet: %s successfully written: %d rows\n%s\n",
			path, parquetWriter.Footer.NumRows, string(footerBytes))
	}

	return nil
}

func validateColTypesMeta(b Bow, values string) (colTypesMeta []parquetColTypesMeta, err error) {
	err = json.Unmarshal([]byte(values), &colTypesMeta)
	if err != nil {
		return nil, fmt.Errorf("invalid column types metadata: %+v", values)
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

func NewMetaWithParquetTimestampMicrosCols(keys, values []string, colNames ...string) Metadata {
	var colTypes = make([]parquetColTypesMeta, len(colNames))

	for i, n := range colNames {
		convertedType := parquet.ConvertedType_TIMESTAMP_MICROS
		logicalType := parquet.LogicalType{
			TIMESTAMP: &parquet.TimestampType{
				IsAdjustedToUTC: true,
				Unit: &parquet.TimeUnit{
					MICROS: &parquet.MicroSeconds{},
				},
			}}
		colTypes[i] = parquetColTypesMeta{
			Name:          n,
			ConvertedType: &convertedType,
			LogicalType:   &logicalType,
		}
	}

	colTypesJSON, err := json.Marshal(colTypes)
	if err != nil {
		panic(fmt.Errorf("NewMetaWithParquetTimestampMicrosCols: %w", err))
	}

	keys = append(keys, keyParquetMetaColTypes)
	values = append(values, string(colTypesJSON))

	return Metadata{arrow.NewMetadata(keys, values)}
}
