package bow

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/tool/parquet-tools/schematool"
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

	//schemaTree := schematool.CreateSchemaTree(schemas)
	//pw, err := writer.NewJSONWriter(outputSchemaTreeJSONSchema(schemaTree), fw, 4)
	pw, err := writer.NewParquetWriter(fw, schemas, 4)
	if err != nil {
		return fmt.Errorf("bow.WriteParquet: writer.NewJSONWriter: %w", err)
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
		if err = write(pw, string(rowJSON)); err != nil {
			return fmt.Errorf("bow.WriteParquet: JSONWriter.Write: %w", err)
		}
	}

	err = writeStop(pw)
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

func writeStop(self *writer.ParquetWriter) error {
	if err := self.Flush(true); err != nil {
		return fmt.Errorf("err1: %w", err)
	}
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	self.RenameSchema()
	footerBuf, err := ts.Write(context.TODO(), self.Footer)
	if err != nil {
		return fmt.Errorf("err2: %w", err)
	}

	if _, err = self.PFile.Write(footerBuf); err != nil {
		return fmt.Errorf("err3: %w", err)
	}
	footerSizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(footerSizeBuf, uint32(len(footerBuf)))

	if _, err = self.PFile.Write(footerSizeBuf); err != nil {
		return fmt.Errorf("err4: %w", err)
	}
	if _, err = self.PFile.Write([]byte("PAR1")); err != nil {
		return fmt.Errorf("err5: %w", err)
	}
	return nil

}

func write(self *writer.ParquetWriter, src interface{}) error {
	var err error
	ln := int64(len(self.Objs))

	val := reflect.ValueOf(src)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
		src = val.Interface()
	}

	if self.CheckSizeCritical <= ln {
		self.ObjSize = (self.ObjSize+common.SizeOf(val))/2 + 1
	}
	self.ObjsSize += self.ObjSize
	self.Objs = append(self.Objs, src)

	criSize := self.NP * self.PageSize * self.SchemaHandler.GetColumnNum()

	if self.ObjsSize >= criSize {
		err = self.Flush(false)

	} else {
		dln := (criSize - self.ObjsSize + self.ObjSize - 1) / self.ObjSize / 2
		self.CheckSizeCritical = dln + ln
	}
	return err

}

type jsonSchemaItemType struct {
	Tag    schemaElement         `json:"tag"`
	Fields []*jsonSchemaItemType `json:"fields"`
}

func newJSONSchemaItem() *jsonSchemaItemType {
	return new(jsonSchemaItemType)
}

type schemaElement struct {
	Name           string `json:"name"`
	Type           string `json:"type,omitempty"`
	RepetitionType string `json:"repetitiontype,omitempty"`
	TypeLength     int32  `json:"length,omitempty"`
	ConvertedType  string `json:"convertedtype,omitempty"`
	Scale          int32  `json:"scale,omitempty"`
	Precision      int32  `json:"precision,omitempty"`
}

func outputSchemaTreeJSONSchema(st *schematool.SchemaTree) string {
	jsonStr := outputNodeJSONSchema(st.Root)
	fmt.Printf("jsonStr\n%s\n", jsonStr)

	sch := newJSONSchemaItem()
	if err := json.Unmarshal([]byte(jsonStr), sch); err != nil {
		panic(err)
	}

	res, err := json.MarshalIndent(&sch, "", "  ")
	if err != nil {
		panic(err)
	}

	fmt.Printf("res\n%s\n", string(res))

	return string(res)
}

func outputNodeJSONSchema(node *schematool.Node) string {
	res := "{\"tag\":"
	pT, cT := node.SE.Type, node.SE.ConvertedType
	rTStr := "REQUIRED"
	if node.SE.GetRepetitionType() == parquet.FieldRepetitionType_OPTIONAL {
		rTStr = "OPTIONAL"
	} else if node.SE.GetRepetitionType() == parquet.FieldRepetitionType_REPEATED {
		rTStr = "REPEATED"
	}

	pTStr, cTStr := schematool.ParquetTypeToParquetTypeStr(pT, cT)
	tagStr := "{\"name\":%q, \"type\":%q, \"repetitiontype\":%q}"

	name := node.SE.GetName()

	if len(node.Children) == 0 {
		if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY && cT == nil {
			length := node.SE.GetTypeLength()
			tagStr = "{\"name\":%q, \"type\":%q, \"length\":%d, \"repetitiontype\":%q}"
			res += fmt.Sprintf(tagStr, name, pTStr, length, rTStr) + "}"
		} else if cT != nil && *cT == parquet.ConvertedType_DECIMAL {
			scale, precision := node.SE.GetScale(), node.SE.GetPrecision()
			if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
				length := node.SE.GetTypeLength()
				tagStr = "{\"name\":%q, \"type\":%q, \"convertedtype\":%q, \"scale\":%d, \"precision\":%d, \"length\":%d, \"repetitiontype\":%q}"
				res += fmt.Sprintf(tagStr, name, pTStr, cTStr, scale, precision, length, rTStr) + "}"
			} else {
				tagStr = "{\"name\":%q, \"type\":%q, \"convertedtype\":%q, \"scale\":%d, \"precision\":%d, \"repetitiontype\":%q}"
				res += fmt.Sprintf(tagStr, name, pTStr, cTStr, scale, precision, rTStr) + "}"
			}
		} else {
			if cT != nil {
				tagStr = "{\"name\":%q, \"type\":%q, \"convertedtype\":%q, \"repetitiontype\":%q}"
				res += fmt.Sprintf(tagStr, name, pTStr, cTStr, rTStr) + "}"
			} else {
				res += fmt.Sprintf(tagStr, name, pTStr, rTStr) + "}"
			}
		}
	} else {
		if cT != nil {
			tagStr = "{\"name\":%q, \"type\":%q, \"repetitiontype\":%q}"
			res += fmt.Sprintf(tagStr, name, cTStr, rTStr)
		} else {
			tagStr = "{\"name\":%q, \"repetitiontype\":%q}"
			res += fmt.Sprintf(tagStr, name, rTStr)
		}
		res += ",\n"
		res += "\"fields\":[\n"

		nodes := node.Children
		if cT != nil {
			nodes = node.Children[0].Children
		}

		for i := 0; i < len(nodes); i++ {
			cNode := nodes[i]
			if i == len(nodes)-1 {
				res += outputNodeJSONSchema(cNode) + "\n"
			} else {
				res += outputNodeJSONSchema(cNode) + ",\n"
			}
		}

		res += "]\n"
		res += "}"
	}

	return res
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
