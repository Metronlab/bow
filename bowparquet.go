package bow

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/marshal"
	"github.com/xitongsys/parquet-go/source"
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
	var typeMeta []parquetColTypesMeta
	for colIndex, col := range pr.Footer.GetSchema() {
		if col.NumChildren != nil {
			continue
		}

		if col.ConvertedType != nil || col.LogicalType != nil {
			typeMeta = append(typeMeta, parquetColTypesMeta{
				Name:          originalColNames[colIndex],
				ConvertedType: col.ConvertedType,
				LogicalType:   col.LogicalType,
			})
		}

		values, _, _, err := pr.ReadColumnByIndex(valueColIndex, pr.GetNumRows())
		if err != nil {
			return nil, fmt.Errorf("bow.NewBowFromParquet: %w", err)
		}

		typ := mapParquetToBowTypes[col.GetType()]
		buf := NewBuffer(len(values), typ)
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

	tmBytes, err := json.Marshal(typeMeta)
	if err != nil {
		panic(fmt.Errorf("bow.NewBowFromParquet: %w", err))
	}

	keys = append(keys, keyParquetMetaColTypes)
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

	var metadata []*parquet.KeyValue
	m := b.Metadata()
	values := m.Values()

	var colTypesMeta []parquetColTypesMeta
	for k, key := range m.Keys() {
		if key != "ARROW:schema" {
			metadata = append(metadata, &parquet.KeyValue{
				Key:   key,
				Value: &values[k],
			})
		}

		if key == keyParquetMetaColTypes {
			var err error
			colTypesMeta, err = validateColTypesMeta(b, values[k])
			if err != nil {
				return fmt.Errorf("bow.WriteParquet: %w", err)
			}
		}
	}

	var numChildren = int32(b.NumCols())
	var requiredRepType = parquet.FieldRepetitionType_REQUIRED
	var schemas []*parquet.SchemaElement
	var logicalTypes = []*parquet.LogicalType{nil}
	se := parquet.NewSchemaElement()
	se.RepetitionType = &requiredRepType
	se.Name = "Schema"
	se.NumChildren = &numChildren
	schemas = append(schemas, se)

	optionalRepType := parquet.FieldRepetitionType_OPTIONAL
	for i, f := range b.Schema().Fields() {
		typ := mapBowToParquetTypes[b.ColumnType(i)]
		se = parquet.NewSchemaElement()
		se.Type = &typ
		se.RepetitionType = &optionalRepType
		se.Name = f.Name
		for j, t := range colTypesMeta {
			if t.Name == f.Name {
				se.ConvertedType = colTypesMeta[j].ConvertedType
				se.LogicalType = colTypesMeta[j].LogicalType
			}
		}
		schemas = append(schemas, se)
		logicalTypes = append(logicalTypes, se.LogicalType)
	}

	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		return fmt.Errorf("bow.WriteParquet: local.NewLocalFileWriter: %w", err)
	}
	defer fw.Close()

	pw, err := newJSONWriter(schemas, fw, 4)
	if err != nil {
		return fmt.Errorf("bow.WriteParquet: newJSONWriter: %w", err)
	}

	pw.Footer.KeyValueMetadata = metadata

	for i, lt := range logicalTypes {
		pw.SchemaHandler.SchemaElements[i].LogicalType = lt
	}

	for row := range b.GetRowsChan() {
		rowJSON, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("bow.WriteParquet: json.Marshal: %w", err)
		}
		if err = pw.Write(string(rowJSON)); err != nil {
			return fmt.Errorf("bow.WriteParquet: JSONWriter.Write: %w", err)
		}
	}

	err = pw.WriteStop()
	if err != nil {
		return fmt.Errorf("bow.WriteParquet: JSONWriter.WriteStop: %w", err)
	}

	footerBytes, err := json.MarshalIndent(pw.Footer, "", "\t")
	if err != nil {
		return fmt.Errorf("bow.WriteParquet: json.MarshalIndent: %w", err)
	}

	if verbose {
		fmt.Printf(
			"bow.WriteParquet: %s successfully written: %d rows\n%s\n",
			path, pw.Footer.NumRows, string(footerBytes))
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

type Elements struct {
	SchemaElement  parquet.SchemaElement `json:"field"`
	SchemaElements []*Elements           `json:"fields,omitempty"`
}

func NewElements() *Elements {
	return new(Elements)
}

func newJSONWriter(se []*parquet.SchemaElement, pfile source.ParquetFile, np int64) (*writer.JSONWriter, error) {
	var err error
	res := new(writer.JSONWriter)
	jsonSchema := schemaTreeToJSONSchema(schematool.CreateSchemaTree(se))
	res.SchemaHandler, err = newSchemaHandlerFromJSONElements(jsonSchema)
	if err != nil {
		return res, fmt.Errorf("newSchemaHandlerFromJSONElements: %w", err)
	}

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
	_, err = res.PFile.Write([]byte("PAR1"))
	res.MarshalFunc = marshal.MarshalJSON
	return res, err
}

func newSchemaHandlerFromJSONElements(str string) (sh *schema.SchemaHandler, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New("string: " + x)
			case error:
				err = x
			default:
				err = errors.New("unknown error")
			}
		}
	}()

	elements := NewElements()
	if err = json.Unmarshal([]byte(str), elements); err != nil {
		return nil, fmt.Errorf("error in unmarshalling json elements string: %w", err)
	}

	stack := make([]*Elements, 0)
	stack = append(stack, elements)
	schemaElements := make([]*parquet.SchemaElement, 0)
	newInfos := make([]*common.Tag, 0)

	for len(stack) > 0 {
		ln := len(stack)
		elem := stack[ln-1]
		stack = stack[:ln-1]

		info := &common.Tag{
			InName: common.StringToVariableName(elem.SchemaElement.Name),
			ExName: elem.SchemaElement.Name,
		}
		if elem.SchemaElement.Type != nil {
			info.Type = elem.SchemaElement.Type.String()
		}
		if elem.SchemaElement.ConvertedType != nil {
			info.ConvertedType = elem.SchemaElement.ConvertedType.String()
		}
		if elem.SchemaElement.RepetitionType != nil {
			info.RepetitionType = *elem.SchemaElement.RepetitionType
		}

		var newInfo *common.Tag
		if info.Type == "" { //struct
			schemaElement := parquet.NewSchemaElement()
			schemaElement.Name = info.InName
			rt := info.RepetitionType
			schemaElement.RepetitionType = &rt
			numField := int32(len(elem.SchemaElements))
			schemaElement.NumChildren = &numField
			schemaElements = append(schemaElements, schemaElement)
			for i := int(numField - 1); i >= 0; i-- {
				newItem := elem.SchemaElements[i]
				stack = append(stack, newItem)
			}
		} else { //normal variable
			schemaElement := common.NewSchemaElementFromTagMap(info)
			schemaElements = append(schemaElements, schemaElement)
		}

		newInfo = common.NewTag()
		common.DeepCopy(info, newInfo)
		newInfos = append(newInfos, newInfo)
	}

	res := schema.NewSchemaHandlerFromSchemaList(schemaElements)
	res.Infos = newInfos
	res.CreateInExMap()

	return res, nil
}

func schemaTreeToJSONSchema(st *schematool.SchemaTree) string {
	jsonStr := nodeToJSONSchema(st.Root)

	var elements Elements
	if err := json.Unmarshal([]byte(jsonStr), &elements); err != nil {
		panic(fmt.Errorf("json.Unmarshal: %w", err))
	}

	res, err := json.MarshalIndent(&elements, "", "\t")
	if err != nil {
		panic(fmt.Errorf("json.Marshal: %w", err))
	}

	return string(res)
}

func nodeToJSONSchema(n *schematool.Node) string {
	res := "{\"field\":"
	pT, cT := n.SE.Type, n.SE.ConvertedType
	rTStr := "REQUIRED"
	if n.SE.GetRepetitionType() == parquet.FieldRepetitionType_OPTIONAL {
		rTStr = "OPTIONAL"
	} else if n.SE.GetRepetitionType() == parquet.FieldRepetitionType_REPEATED {
		rTStr = "REPEATED"
	}

	pTStr, cTStr := schematool.ParquetTypeToParquetTypeStr(pT, cT)
	tagStr := "{\"name\":%q, \"type\":%q, \"repetition_type\":%q}"

	name := n.SE.GetName()

	if len(n.Children) == 0 {
		if cT != nil {
			tagStr = "{\"name\":%q, \"type\":%q, \"converted_type\":%q, \"repetition_type\":%q}"
			res += fmt.Sprintf(tagStr, name, pTStr, cTStr, rTStr) + "}"
		} else {
			res += fmt.Sprintf(tagStr, name, pTStr, rTStr) + "}"
		}
	} else {
		if cT != nil {
			tagStr = "{\"name\":%q, \"type\":%q, \"repetition_type\":%q}"
			res += fmt.Sprintf(tagStr, name, cTStr, rTStr)
		} else {
			tagStr = "{\"name\":%q, \"repetition_type\":%q}"
			res += fmt.Sprintf(tagStr, name, rTStr)
		}
		res += ",\n"
		res += "\"fields\":[\n"

		nodes := n.Children
		if cT != nil {
			nodes = n.Children[0].Children
		}

		for i := 0; i < len(nodes); i++ {
			cNode := nodes[i]
			if i == len(nodes)-1 {
				res += nodeToJSONSchema(cNode) + "\n"
			} else {
				res += nodeToJSONSchema(cNode) + ",\n"
			}
		}

		res += "]\n"
		res += "}"
	}

	return res
}
