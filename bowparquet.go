package bow

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/arrow"
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

func NewBowFromParquet(fileName string) (Bow, error) {
	fr, err := local.NewLocalFileReader(fileName)
	if err != nil {
		fr, err = local.NewLocalFileReader(fileName + ".parquet")
		if err != nil {
			return nil, fmt.Errorf("bow.NewBowFromParquet: %w", err)
		}
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
	for colIndex, col := range pr.Footer.GetSchema() {
		if col.NumChildren != nil {
			continue
		}

		values, _, _, err := pr.ReadColumnByIndex(valueColIndex, pr.GetNumRows())
		if err != nil {
			return nil, fmt.Errorf("bow.NewBowFromParquet: %w", err)
		}

		var ok bool
		switch TypeParquetToBowMap[col.GetType()] {
		case Int64:
			var vs = make([]int64, len(values))
			for i, v := range values {
				vs[i], ok = ToInt64(v)
				if !ok {
					panic(fmt.Errorf("bow.NewBowFromParquet: error while converting values: %+v", values))
				}
			}
			series[valueColIndex] = NewSeries(originalColNames[colIndex], Int64, vs, nil)

		case Float64:
			var vs = make([]float64, len(values))
			for i, v := range values {
				vs[i], ok = ToFloat64(v)
				if !ok {
					panic(fmt.Errorf("bow.NewBowFromParquet: error while converting values: %+v", values))
				}
			}
			series[valueColIndex] = NewSeries(originalColNames[colIndex], Float64, vs, nil)

		case Bool:
			var vs = make([]bool, len(values))
			for i, v := range values {
				vs[i], ok = ToBool(v)
				if !ok {
					panic(fmt.Errorf("bow.NewBowFromParquet: error while converting values: %+v", values))
				}
			}
			series[valueColIndex] = NewSeries(originalColNames[colIndex], Bool, vs, nil)

		case String:
			var vs = make([]string, len(values))
			for i, v := range values {
				vs[i], ok = ToString(v)
				if !ok {
					panic(fmt.Errorf("bow.NewBowFromParquet: error while converting values: %+v", values))
				}
			}
			series[valueColIndex] = NewSeries(originalColNames[colIndex], String, vs, nil)

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
		if m.GetKey() != "ARROW:schema" {
			keys = append(keys, m.GetKey())
			values = append(values, m.GetValue())
		}
	}
	metadata := arrow.NewMetadata(keys, values)

	b, err := NewBow(&metadata, series...)
	if err != nil {
		return nil, fmt.Errorf("bow.NewBowFromParquet: %w", err)
	}

	footerIndented, err := json.MarshalIndent(pr.Footer, "", "\t")
	if err != nil {
		return nil, fmt.Errorf("bow.NewBowFromParquet: %w", err)
	}

	fmt.Printf("bow.NewBowFromParquet: %s successfully read: %d rows\n%+v\n%+v\n",
		fileName, b.NumRows(), b.Schema().String(), string(footerIndented))

	return b, nil
}

func (b *bow) WriteParquet(fileName string) error {
	if b.NumCols() == 0 {
		return fmt.Errorf("bow.WriteParquet: no columns")
	}

	if !strings.HasSuffix(fileName, ".parquet") {
		fileName += ".parquet"
	}

	fw, err := local.NewLocalFileWriter(fileName)
	if err != nil {
		return fmt.Errorf("bow.WriteParquet: %w", err)
	}
	defer fw.Close()

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
		schemas = append(schemas, se)
	}

	schemaTree := schematool.CreateSchemaTree(schemas)
	pw, err := writer.NewJSONWriter(schemaTree.OutputJsonSchema(), fw, 4)
	if err != nil {
		return fmt.Errorf("bow.WriteParquet: %w", err)
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

	var metadata []*parquet.KeyValue
	keys := b.Schema().Metadata().Keys()
	values := b.Schema().Metadata().Values()
	for k, key := range keys {
		if key != "ARROW:schema" {
			metadata = append(metadata, &parquet.KeyValue{
				Key:   key,
				Value: &values[k],
			})
		}
	}
	pw.Footer.KeyValueMetadata = metadata

	err = pw.WriteStop()
	if err != nil {
		return fmt.Errorf("bow.WriteParquet: %w", err)
	}

	footerBytes, err := json.MarshalIndent(pw.Footer, "", "\t")
	if err != nil {
		return fmt.Errorf("bow.WriteParquet: %w", err)
	}

	fmt.Printf("bow.WriteParquet: %s successfully written: %d rows\n%s\n",
		fileName, pw.Footer.NumRows, string(footerBytes))

	return nil
}
