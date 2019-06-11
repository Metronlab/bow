package bow

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"text/tabwriter"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

//Bow is a wrapper of apache arrow array record.
// It was not implemented as a facade shadowing arrow
// in order to expose low lvl arrow decisions to bow users
// while arrow is in beta
type Bow interface {
	GetType(colIndex int) (Type, error)
	GetName(colIndex int) string
	GetValue(colIndex, rowIndex int) interface{}
	GetValueByName(colName string, rowIndex int) interface{}
	GetRow(rowIndex int) map[string]interface{}

	// stringer interface for printing rows
	String() string

	RowMapIter() chan map[string]interface{}
	IntervalRolling(column int, interval int64, options RollingOptions) (Rolling, error)

	InnerJoin(b2 Bow) Bow

	Equal(Bow) bool
	// todo: design and rethink:
	// Merge(bows ...Bow) (Bow, error)

	SetMarshalJSONRowBased(rowOriented bool)
	MarshalJSON() ([]byte, error)
	UnmarshalJSON([]byte) error

	// Surcharged on Record:
	NewSlice(i, j int64) Bow
	NewColumns(columns [][]interface{}) (bobow Bow, err error)
	NewEmpty() Bow

	// Exposed from Record:
	Release()
	Retain()

	NumRows() int64
	NumCols() int64
	NumSchemaCols() int
}

type bow struct {
	indexes             map[string]index
	marshalJSONRowBased bool

	array.Record
}

func NewBow(series ...Series) (Bow, error) {
	record, err := newRecordFromSeries(series...)
	if err != nil {
		return nil, err
	}

	return &bow{
		Record: record,
	}, nil
}

func NewBowFromColumnBasedInterfaces(columnsNames []string, types []Type, columns [][]interface{}) (bobow Bow, err error) {
	if len(columnsNames) != len(columns) {
		return nil, errors.New("bow: columnsNames name and values doesn't match")
	}

	if types != nil && len(columnsNames) != len(types) {
		return nil, errors.New("bow: columnsNames name and types doesn't match")
	}

	series := make([]Series, len(columnsNames))
	for i, name := range columnsNames {
		if types != nil {
			series[i], err = NewSeriesFromInterfaces(name, types[i], columns[i])
		} else {
			series[i], err = NewSeriesFromInterfaces(name, Unknown, columns[i])
		}
		if err != nil {
			return
		}
	}
	return NewBow(series...)
}

func NewBowFromRowBasedInterfaces(columnsNames []string, types []Type, rows [][]interface{}) (bobow Bow, err error) {
	if len(rows) <= 0 {
		return nil, errors.New("bow: empty rows")
	}
	columnBasedRows := make([][]interface{}, len(columnsNames))
	for column := range columnsNames {
		columnBasedRows[column] = make([]interface{}, len(rows))
	}
	for rowI, row := range rows {
		if len(columnsNames) < len(row) {
			return nil, errors.New("bow: mismatch between columnsNames names and row len")
		}
		for colI := range columnsNames {
			columnBasedRows[colI][rowI] = row[colI]
		}
	}
	return NewBowFromColumnBasedInterfaces(columnsNames, types, columnBasedRows)
}

func (b *bow) NewEmpty() Bow {
	return b.NewSlice(0, 0)
}

func (b *bow) NewColumns(columns [][]interface{}) (Bow, error) {
	if len(columns) != b.NumSchemaCols() {
		return nil, errors.New("bow: mismatch between schema and data")
	}
	seriess := make([]Series, len(columns))
	for i, c := range columns {
		typ, err := getTypeFromArrowType(b.Schema().Field(i).Type.Name())
		if err != nil {
			return nil, err
		}
		buf, err := NewBufferFromInterfaces(typ, c)
		if err != nil {
			return nil, err
		}
		seriess[i] = Series{
			Name: b.Schema().Field(i).Name,
			Type: typ,
			Data: buf,
		}
	}
	return NewBow(seriess...)
}

func (b *bow) String() string {
	if b.Record == nil {
		return ""
	}
	w := new(tabwriter.Writer)
	writer := new(strings.Builder)
	// tabs will be replaced by two spaces by formater
	w.Init(writer, 0, 4, 2, ' ', 0)

	// format any line (header or row)
	formatRow := func(getCellStr func(colIndex int) string) {
		var cells []string
		for colIndex := 0; colIndex < int(b.NumCols()); colIndex++ {
			cells = append(cells, fmt.Sprintf("%v", getCellStr(colIndex)))
		}
		_, err := fmt.Fprintln(w, strings.Join(cells, "\t"))
		if err != nil {
			panic(err)
		}
	}

	// Print col names on buffer
	formatRow(func(colIndex int) string {
		return b.Schema().Field(colIndex).Name
	})

	// Print each row on buffer
	rowChan := b.RowMapIter()
	for row := range rowChan {
		formatRow(func(colIndex int) string {
			return fmt.Sprintf("%v", row[b.Schema().Field(colIndex).Name])
		})
	}

	// Flush buffer and format lines along the way
	if err := w.Flush(); err != nil {
		panic(err)
	}

	return writer.String()
}

func (b *bow) RowMapIter() chan map[string]interface{} {
	mapChan := make(chan map[string]interface{})
	go b.rowMapIter(mapChan)
	return mapChan
}

func (b *bow) rowMapIter(mapChan chan map[string]interface{}) {
	defer close(mapChan)

	if b.Record == nil || b.NumRows() <= 0 {
		return
	}

	for rowIndex := 0; rowIndex < int(b.NumRows()); rowIndex++ {
		mapChan <- b.GetRow(rowIndex)
	}
}

func (b *bow) GetRow(rowIndex int) map[string]interface{} {
	m := map[string]interface{}{}
	for colIndex := 0; colIndex < int(b.NumCols()); colIndex++ {
		val := b.GetValue(colIndex, rowIndex)
		if val == nil {
			continue
		}
		m[b.Schema().Field(colIndex).Name] = val
	}
	return m
}

func (b *bow) GetValueByName(colName string, rowIndex int) interface{} {
	for colIndex := 0; colIndex < int(b.NumCols()); colIndex++ {
		name := b.Schema().Field(colIndex).Name
		if colName == name {
			return b.GetValue(colIndex, rowIndex)
		}
	}
	return nil
}

func (b *bow) GetValue(colIndex, rowIndex int) interface{} {
	switch b.Schema().Field(colIndex).Type.ID() {
	case arrow.FLOAT64:
		vd := array.NewFloat64Data(b.Column(colIndex).Data())
		if vd.IsValid(rowIndex) {
			return vd.Value(rowIndex)
		}
	case arrow.INT64:
		vd := array.NewInt64Data(b.Column(colIndex).Data())
		if vd.IsValid(rowIndex) {
			return vd.Value(rowIndex)
		}
	case arrow.BOOL:
		vd := array.NewBooleanData(b.Column(colIndex).Data())
		if vd.IsValid(rowIndex) {
			return vd.Value(rowIndex)
		}
	default:
		panic(fmt.Sprintf("bow: unhandled type %s",
			b.Schema().Field(colIndex).Type.Name()))
	}
	return nil
}

func (b *bow) GetType(colIndex int) (Type, error) {
	return getTypeFromArrowType(b.Schema().Field(colIndex).Type.Name())
}

func (b *bow) GetName(colIndex int) string {
	return b.Schema().Field(colIndex).Name
}

func (b *bow) InnerJoin(B2 Bow) Bow {
	b2, ok := B2.(*bow)
	if !ok {
		panic("bow: non bow object pass as argument")
	}

	// build indexing over column names
	commonColumns, err := b.seekCommonColumnsNames(b2)
	if err != nil {
		panic(err)
	}

	// build leftOver indexes from b2
	var rColIndexes []int
	for i, rField := range b2.Schema().Fields() {
		if _, ok := commonColumns[rField.Name]; !ok {
			rColIndexes = append(rColIndexes, i)
		}
	}

	for name := range commonColumns {
		b2.newIndex(name)
	}

	resultInterfaces := b.innerJoinInColumnBaseInterfaces(b2, commonColumns, rColIndexes)

	columnNames, columnsTypes := b.makeColNamesAndTypesOnJoin(b2, commonColumns, rColIndexes)

	res, err := NewBowFromColumnBasedInterfaces(columnNames, columnsTypes, resultInterfaces)
	if err != nil {
		panic(err)
	}
	return res
}

//innerJoinInColumnBaseInterfaces create a column based interface transitory dataframe.
// TODO: used series directly
// For each resulting row, every values is filled first with all left bow columns then right uncommon columns
// If several values are present on right on same indexes, the left indexes/values will be duplicated
// left bow:         right bow:
// index col         index col2
// 1     1           1     1
//                   1     2
// result:
// index col col2
// 1     1   1
// 1     1   2
func (b *bow) innerJoinInColumnBaseInterfaces(b2 *bow, commonColumns map[string]struct{}, rColIndexes []int) [][]interface{} {
	resultInterfaces := make([][]interface{}, len(b.Schema().Fields())+len(rColIndexes))
	for rowIndex := int64(0); rowIndex < b.NumRows(); rowIndex++ {
		for _, rValIndex := range b.getRightBowIndexesAtRow(b2, commonColumns, rowIndex) {
			for colIndex := range b.Schema().Fields() {
				resultInterfaces[colIndex] = append(resultInterfaces[colIndex], b.GetValue(colIndex, int(rowIndex)))
			}
			for i, rColIndex := range rColIndexes {
				resultInterfaces[len(b.Schema().Fields())+i] =
					append(resultInterfaces[len(b.Schema().Fields())+i], b2.GetValue(rColIndex, rValIndex))
			}
		}
	}
	return resultInterfaces
}

func (b *bow) seekCommonColumnsNames(b2 *bow) (map[string]struct{}, error) {
	commonColumns := map[string]struct{}{}
	for _, lField := range b.Schema().Fields() {
		rField, ok := b2.Schema().FieldByName(lField.Name)
		if !ok {
			continue
		}
		if rField.Type.ID() != lField.Type.ID() {
			return nil, errors.New("bow: left and right bow on join columns are of incompatible types: " + lField.Name)
		}
		commonColumns[lField.Name] = struct{}{}

	}
	return commonColumns, nil
}

func keysEquals(l, r map[string]interface{}, columnNames map[string]struct{}) bool {
	for name := range columnNames {
		if !reflect.DeepEqual(l[name], r[name]) {
			return false
		}
	}
	return true
}

func (b *bow) makeColNamesAndTypesOnJoin(
	b2 *bow, commonColumns map[string]struct{}, rColNotInLIndexes []int) ([]string, []Type) {
	var err error
	colNames := make([]string, len(b.Schema().Fields())+len(rColNotInLIndexes))
	colType := make([]Type, len(b.Schema().Fields())+len(rColNotInLIndexes))
	for i, f := range b.Schema().Fields() {
		colNames[i] = f.Name
		if colType[i], err = getTypeFromArrowType(f.Type.Name()); err != nil {
			panic(err)
		}
	}
	for i, index := range rColNotInLIndexes {
		colNames[len(b.Schema().Fields())+i] = b2.Schema().Field(index).Name
		if colType[len(b.Schema().Fields())+i], err = getTypeFromArrowType(b2.Schema().Field(index).Type.Name()); err != nil {
			panic(err)
		}
	}
	return colNames, colType
}

func (b *bow) Equal(B2 Bow) bool {
	b2, ok := B2.(*bow)
	if !ok {
		panic("bow: cannot Equal on non bow object")
	}

	if b.Record == nil && b2.Record == nil {
		return true
	}
	if b.Record == nil && b2.Record != nil {
		return false
	}
	if b2.Record == nil && b.Record != nil {
		return false
	}

	if !b.Schema().Equal(b2.Schema()) {
		return false
	}

	b1Chan := b.RowMapIter()
	b2Chan := b2.RowMapIter()
	for {
		i1, ok1 := <-b1Chan
		i2, ok2 := <-b2Chan
		for (i1 == nil || len(i1) == 0) && ok1 {
			i1, ok1 = <-b1Chan
		}
		for (i2 == nil || len(i2) == 0) && ok2 {
			i2, ok2 = <-b2Chan
		}
		if ok1 != ok2 {
			return false
		}
		if !ok1 && !ok2 {
			break
		}
		if !reflect.DeepEqual(i1, i2) {
			return false
		}
	}
	return true
}

func (b *bow) SetMarshalJSONRowBased(rowOriented bool) {
	b.marshalJSONRowBased = rowOriented
}

func (b *bow) MarshalJSON() ([]byte, error) {
	if b.marshalJSONRowBased {
		rowBased := struct {
			ColumnsTypes map[string]string        `json:"columnsTypes"`
			Rows         []map[string]interface{} `json:"rows"`
		}{ColumnsTypes: map[string]string{}}
		for row := range b.RowMapIter() {
			if len(row) == 0 {
				continue
			}
			rowBased.Rows = append(rowBased.Rows, row)
		}
		for _, col := range b.Schema().Fields() {
			rowBased.ColumnsTypes[col.Name] = col.Type.Name()
		}
		return json.Marshal(rowBased)
	}
	panic("bow: column based json marshaller not implemented")
}

func (b *bow) UnmarshalJSON(data []byte) error {
	jsonBow := struct {
		// Columns specifics
		Columns map[string]interface{} `json:"columns"`

		// Rows specifics
		ColumnsTypes map[string]string        `json:"columnsTypes"`
		Rows         []map[string]interface{} `json:"rows"`
	}{}

	if err := json.Unmarshal(data, &jsonBow); err != nil {
		return err
	}
	if jsonBow.Columns != nil {
		panic("bow: column based json unMarshaller not implemented")
	} else {
		b.SetMarshalJSONRowBased(true)
		series := make([]Series, len(jsonBow.ColumnsTypes))
		i := 0
		for colName, ArrowTypeName := range jsonBow.ColumnsTypes {
			t, err := getTypeFromArrowType(ArrowTypeName)
			if err != nil {
				return err
			}
			buf, err := NewBufferFromInterfacesIter(t, len(jsonBow.Rows), func() chan interface{} {
				cellsChan := make(chan interface{})
				go func(cellsChan chan interface{}, colName string) {
					for _, row := range jsonBow.Rows {
						val, ok := row[colName]
						if !ok {
							cellsChan <- nil
						} else {
							_, ok = val.(float64)
							if t == Int64 && ok {
								val = int64(val.(float64))
							}
							cellsChan <- val
						}
					}
					close(cellsChan)
				}(cellsChan, colName)
				return cellsChan
			}())
			if err != nil {
				return err
			}
			series[i] = NewSeries(colName, t, buf.Value, buf.Valid)
			i++
		}
		tmpBow, err := NewBow(series...)
		if err != nil {
			return err
		}
		b.Record = tmpBow.(*bow).Record
	}

	return nil
}

func (b *bow) NewSlice(i, j int64) Bow {
	return &bow{
		Record: b.Record.NewSlice(i, j),
	}
}

func (b *bow) NumRows() int64 {
	if b.Record == nil {
		return 0
	}
	return b.Record.NumRows()
}

func (b *bow) NumCols() int64 {
	if b.Record == nil {
		return 0
	}
	return b.Record.NumCols()
}

// NumSchemaCols counts columns based on schema fields,
// independently of data written.
func (b *bow) NumSchemaCols() int {
	return len(b.Schema().Fields())
}
