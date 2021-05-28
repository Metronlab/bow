package bow

import (
	"encoding/json"
	"fmt"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/tool/parquet-tools/schematool"
	"github.com/xitongsys/parquet-go/writer"
	"os"
	"strings"
)

var ParquetToBowType = map[parquet.Type]Type{
	parquet.Type_BOOLEAN:    Bool,
	parquet.Type_INT64:      Int64,
	parquet.Type_DOUBLE:     Float64,
	parquet.Type_BYTE_ARRAY: String,
}

var BowToParquetType = map[Type]parquet.Type{
	Bool:    parquet.Type_BOOLEAN,
	Int64:   parquet.Type_INT64,
	Float64: parquet.Type_DOUBLE,
	String:  parquet.Type_BYTE_ARRAY,
}

func NewBowFromParquet(fileName string) (Bow, error) {
	r, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	fr, err := local.NewLocalFileReader(fileName)
	if err != nil {
		panic(err)
	}

	pr, err := reader.NewParquetColumnReader(fr, 4)
	if err != nil {
		panic(err)
	}

	series := make([]Series, pr.SchemaHandler.GetColumnNum())

	var valueColIndex int64
	schema := pr.Footer.GetSchema()

	/*
		tree := schematool.CreateSchemaTree(pr.SchemaHandler.SchemaElements)
		fmt.Printf("Read SchemaTree\n%s\n", tree.OutputJsonSchema())

		footerBytes, err := json.MarshalIndent(pr.Footer, "", "\t")
		if err != nil {
			return nil, err
		}
		fmt.Printf("footer\n%s\n", string(footerBytes))
	*/

	for _, col := range schema {
		if col.NumChildren == nil {
			values, _, _, err := pr.ReadColumnByIndex(valueColIndex, pr.GetNumRows())
			if err != nil {
				panic(err)
			}

			var ok bool
			switch ParquetToBowType[col.GetType()] {
			case Int64:
				var vs = make([]int64, len(values))
				for i, v := range values {
					vs[i], ok = ToInt64(v)
					if !ok {
						panic(values)
					}
				}
				series[valueColIndex] = NewSeries(col.Name, Int64, vs, nil)

			case Float64:
				var vs = make([]float64, len(values))
				for i, v := range values {
					vs[i], ok = ToFloat64(v)
					if !ok {
						panic(values)
					}
				}
				series[valueColIndex] = NewSeries(col.Name, Float64, vs, nil)

			case Bool:
				var vs = make([]bool, len(values))
				for i, v := range values {
					vs[i], ok = ToBool(v)
					if !ok {
						panic(values)
					}
				}
				series[valueColIndex] = NewSeries(col.Name, Bool, vs, nil)

			case String:
				var vs = make([]string, len(values))
				for i, v := range values {
					vs[i], ok = ToString(v)
					if !ok {
						panic(values)
					}
				}
				series[valueColIndex] = NewSeries(col.Name, String, vs, nil)

			default:
				return nil, fmt.Errorf("unsupported type %s", col.GetType())
			}
			valueColIndex++
		}
	}

	return NewBow(series...)
}

func (b *bow) WriteParquet(fileName string) error {
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
		typ := BowToParquetType[b.GetType(i)]
		se = parquet.NewSchemaElement()
		se.Type = &typ
		se.RepetitionType = &optionalRepType
		se.Name = f.Name
		schemas = append(schemas, se)
	}

	schemaTree := schematool.CreateSchemaTree(schemas)
	//fmt.Printf("Write SchemaTree\n%s\n", schemaTree.OutputJsonSchema())

	pw, err := writer.NewJSONWriter(schemaTree.OutputJsonSchema(), fw, 4)
	if err != nil {
		return fmt.Errorf("bow.WriteParquet: %w", err)
	}
	pw.NumRows = int64(b.NumRows())

	for row := range b.RowMapIter() {
		rowJSON, err := json.Marshal(row)
		if err = pw.Write(string(rowJSON)); err != nil {
			return fmt.Errorf("bow.WriteParquet: %w", err)
		}
	}

	if err = pw.WriteStop(); err != nil {
		return fmt.Errorf("bow.WriteParquet: %w", err)
	}

	footerBytes, err := json.MarshalIndent(pw.Footer, "", "\t")
	if err != nil {
		return err
	}

	fmt.Printf("bow.WriteParquet: %s successfully written\n%s",
		fileName, string(footerBytes))

	return nil
}
