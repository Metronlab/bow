package bow

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/tool/parquet-tools/schematool"
	"github.com/xitongsys/parquet-go/writer"
)

var TypeParquetToBowMap = map[parquet.Type]Type{
	parquet.Type_BOOLEAN:    Bool,
	parquet.Type_INT64:      Int64,
	parquet.Type_DOUBLE:     Float64,
	parquet.Type_BYTE_ARRAY: String,
}

var TypeBowToParquetMap = map[Type]parquet.Type{
	Bool:    parquet.Type_BOOLEAN,
	Int64:   parquet.Type_INT64,
	Float64: parquet.Type_DOUBLE,
	String:  parquet.Type_BYTE_ARRAY,
}

const KeyColTypeMetadata = "col_types"

type colTypeMetadata struct {
	Name          string                 `json:"name"`
	ConvertedType *parquet.ConvertedType `json:"converted_type"`
	LogicalType   *parquet.LogicalType   `json:"logical_type"`
}

// NewBowFromParquet loads a parquet object from the file path, returning a new Bow
// Argument verbose is used to print information about the file loaded.
func NewBowFromParquet(path string, verbose bool) (Bow, error) {
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		return nil, fmt.Errorf("bow.NewBowFromParquet: %w", err)
	}

	pr := new(reader.ParquetReader)
	pr.NP = 4
	pr.PFile = fr
	if err := pr.ReadFooter(); err != nil {
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
	var typeMeta []colTypeMetadata
	for colIndex, col := range pr.Footer.GetSchema() {
		if col.NumChildren != nil {
			continue
		}

		typeMeta = append(typeMeta, colTypeMetadata{
			Name:          originalColNames[colIndex],
			ConvertedType: col.ConvertedType,
			LogicalType:   col.LogicalType,
		})

		values, _, _, err := pr.ReadColumnByIndex(valueColIndex, pr.GetNumRows())
		if err != nil {
			return nil, fmt.Errorf("bow.NewBowFromParquet: %w", err)
		}

		var ok bool
		var vd = make([]bool, len(values))
		switch TypeParquetToBowMap[col.GetType()] {
		case Int64:
			var vs = make([]int64, len(values))
			for i, v := range values {
				vs[i], ok = ToInt64(v)
				if ok {
					vd[i] = true
				}
			}
			series[valueColIndex] = NewSeries(originalColNames[colIndex], Int64, vs, vd)

		case Float64:
			var vs = make([]float64, len(values))
			for i, v := range values {
				vs[i], ok = ToFloat64(v)
				if ok {
					vd[i] = true
				}
			}
			series[valueColIndex] = NewSeries(originalColNames[colIndex], Float64, vs, vd)

		case Bool:
			var vs = make([]bool, len(values))
			for i, v := range values {
				vs[i], ok = ToBool(v)
				if ok {
					vd[i] = true
				}
			}
			series[valueColIndex] = NewSeries(originalColNames[colIndex], Bool, vs, vd)

		case String:
			var vs = make([]string, len(values))
			for i, v := range values {
				vs[i], ok = ToString(v)
				if ok {
					vd[i] = true
				}
			}
			series[valueColIndex] = NewSeries(originalColNames[colIndex], String, vs, vd)

		default:
			return nil, fmt.Errorf("bow.NewBowFromParquet: unsupported type %s", col.GetType())
		}
		pr.Footer.Schema[colIndex].Name = originalColNames[colIndex]
		valueColIndex++
	}

	for r, rg := range pr.Footer.RowGroups {
		for c := range rg.Columns {
			pr.Footer.RowGroups[r].Columns[c].MetaData.PathInSchema = originalRowGroups[r].Columns[c].MetaData.PathInSchema
		}
	}

	var keys, values []string
	for _, m := range pr.Footer.KeyValueMetadata {
		if m.GetKey() != "ARROW:schema" && m.GetKey() != KeyColTypeMetadata {
			keys = append(keys, m.GetKey())
			values = append(values, m.GetValue())
		}
	}

	tmBytes, err := json.Marshal(typeMeta)
	if err != nil {
		panic(fmt.Errorf("bow.NewBowFromParquet: %w", err))
	}

	keys = append(keys, KeyColTypeMetadata)
	values = append(values, string(tmBytes))

	b, err := NewBowWithMetadata(NewMetadata(keys, values), series...)
	if err != nil {
		return nil, fmt.Errorf("bow.NewBowFromParquet: %w", err)
	}

	footerIndented, err := json.MarshalIndent(pr.Footer, "", "\t")
	if err != nil {
		return nil, fmt.Errorf("bow.NewBowFromParquet: %w", err)
	}

	if verbose {
		fmt.Printf("bow.NewBowFromParquet: %s successfully read: %d rows\n%+v\n%+v\n",
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

	var metadata []*parquet.KeyValue
	m := b.GetMetadata()
	values := m.Values()

	var typeMeta []colTypeMetadata
	for k, key := range m.Keys() {
		if key != "ARROW:schema" {
			metadata = append(metadata, &parquet.KeyValue{
				Key:   key,
				Value: &values[k],
			})
		}

		if key == KeyColTypeMetadata {
			err := json.Unmarshal([]byte(values[k]), &typeMeta)
			if err != nil {
				return fmt.Errorf("bow.WriteParquet: %w", err)
			}

			if len(typeMeta) != len(b.Schema().Fields()) {
				return fmt.Errorf("bow.WriteParquet: invalid column type metadata: %+v", typeMeta)
			}
		}
	}

	var numChildren = int32(b.NumCols())
	var requiredRepType = parquet.FieldRepetitionType_REQUIRED
	var schemas []*parquet.SchemaElement
	se := parquet.NewSchemaElement()
	se.RepetitionType = &requiredRepType
	se.Name = "Schema"
	se.NumChildren = &numChildren
	schemas = append(schemas, se)

	optionalRepType := parquet.FieldRepetitionType_OPTIONAL
	for i, f := range b.Schema().Fields() {
		typ := TypeBowToParquetMap[b.GetType(i)]
		se = parquet.NewSchemaElement()
		se.Type = &typ
		se.RepetitionType = &optionalRepType
		se.Name = f.Name
		if len(typeMeta) > 0 {
			se.ConvertedType = typeMeta[i].ConvertedType
			se.LogicalType = typeMeta[i].LogicalType
		}
		schemas = append(schemas, se)
	}

	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		return fmt.Errorf("bow.WriteParquet: %w", err)
	}
	defer fw.Close()

	schemaTree := schematool.CreateSchemaTree(schemas)
	pw, err := writer.NewJSONWriter(schemaTree.OutputJsonSchema(), fw, 4)
	if err != nil {
		return fmt.Errorf("bow.WriteParquet: %w", err)
	}
	pw.Footer.KeyValueMetadata = metadata

	for i := 1; len(typeMeta) > 0 && i < len(pw.SchemaHandler.SchemaElements); i++ {
		pw.SchemaHandler.SchemaElements[i].LogicalType = typeMeta[i-1].LogicalType
	}

	for row := range b.RowMapIter() {
		rowJSON, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("bow.WriteParquet: %w", err)
		}
		if err = pw.Write(string(rowJSON)); err != nil {
			return fmt.Errorf("bow.WriteParquet: %w", err)
		}
	}

	err = pw.WriteStop()
	if err != nil {
		return fmt.Errorf("bow.WriteParquet: %w", err)
	}

	footerBytes, err := json.MarshalIndent(pw.Footer, "", "\t")
	if err != nil {
		return fmt.Errorf("bow.WriteParquet: %w", err)
	}

	if verbose {
		fmt.Printf("bow.WriteParquet: %s successfully written: %d rows\n%s\n",
			path, pw.Footer.NumRows, string(footerBytes))
	}

	return nil
}
