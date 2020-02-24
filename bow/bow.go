package bow

import (
	"errors"
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"reflect"
	"strings"
	"text/tabwriter"
)

//Bow is a wrapper of apache arrow array record.
// It was not implemented as a facade shadowing arrow
// in order to expose low lvl arrow decisions to bow users
// while arrow is in beta
type Bow interface {
	// meet Stringer interface
	String() string

	// Getters
	GetColNameIndex(string) int

	GetType(colIndex int) Type
	GetName(colIndex int) (string, error)
	GetIndex(colName string) (int, error)

	GetRow(rowIndex int) map[string]interface{}

	GetValueByName(colName string, rowIndex int) interface{}
	GetValue(colIndex, rowIndex int) interface{}
	GetNextValue(col, row int) (v interface{}, resultsRow int)
	GetNextValues(col1, col2, row int) (v1, v2 interface{}, resultsRow int)
	GetPreviousValue(col, row int) (v interface{}, resultsRow int)
	GetPreviousValues(col1, col2, row int) (v1, v2 interface{}, resultsRow int)

	GetInt64(colIndex, rowIndex int) (int64, bool)
	GetPreviousInt64(col, row int) (v int64, resultsRow int)

	GetFloat64(colIndex, rowIndex int) (float64, bool)
	GetNextFloat64(col, row int) (v float64, resultsRow int)
	GetNextFloat64s(col1, col2, row int) (v1, v2 float64, resultsRow int)
	GetPreviousFloat64(col, row int) (v float64, resultsRow int)
	GetPreviousFloat64s(col1, col2, row int) (v1, v2 float64, resultsRow int)

	// Iterators

	RowMapIter() chan map[string]interface{}

	InnerJoin(b2 Bow) Bow

	Equal(Bow) bool
	// todo: design and rethink:
	// Merge(bows ...Bow) (Bow, error)

	SetMarshalJSONRowBased(rowOriented bool)
	MarshalJSON() ([]byte, error)
	UnmarshalJSON([]byte) error

	NewSlice(i, j int) Bow
	NewValues(columns [][]interface{}) (bobow Bow, err error)
	NewEmpty() Bow
	DropNil(nilCols ...string) (Bow, error)

	// Handling missing data
	FillPrevious(colNames ...string) (bobow Bow, err error)
	FillNext(colNames ...string) (bobow Bow, err error)
	FillLinear(colNames ...string) (bobow Bow, err error)

	// Exposed from Record:
	Release()
	Retain()
	Schema() *arrow.Schema
	Column(i int) array.Interface

	NumRows() int
	NumCols() int
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

func AppendBows(bows ...Bow) (Bow, error) {
	if len(bows) == 0 {
		return nil, nil
	}
	if len(bows) == 1 {
		return bows[0], nil
	}

	refBow := bows[0]
	refSchema := refBow.Schema()
	var numRows int
	for _, b := range bows {
		schema := b.Schema()
		if !schema.Equal(refSchema) {
			return nil, fmt.Errorf("schema mismatch: got both\n%v\nand\n%v", refSchema, schema)
		}
		numRows += b.NumRows()
	}

	seriess := make([]Series, refBow.NumCols())
	bufs := make([]Buffer, refBow.NumCols())

	for ci := 0; ci < refBow.NumCols(); ci++ {
		var rowOffset int
		typ := refBow.GetType(ci)
		name, err := refBow.GetName(ci)
		if err != nil {
			return nil, err
		}

		bufs[ci] = NewBuffer(numRows, typ, true)
		for _, b := range bows {
			for ri := 0; ri < b.NumRows(); ri++ {
				bufs[ci].SetOrDrop(ri+rowOffset, b.GetValue(ci, ri))
			}
			rowOffset += b.NumRows()
		}

		seriess[ci] = NewSeries(name, typ, bufs[ci].Value, bufs[ci].Valid)
	}

	return NewBow(seriess...)
}

func (b *bow) NewEmpty() Bow {
	return b.NewSlice(0, 0)
}

func (b *bow) NewValues(columns [][]interface{}) (Bow, error) {
	if len(columns) != b.NumCols() {
		return nil, errors.New("bow: mismatch between schema and data")
	}
	seriess := make([]Series, len(columns))
	for i, c := range columns {
		typ := b.GetType(i)
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

// DropNil drops any row that contains a nil for any of `nilCols`.
// `nilCols` defaults to all columns.
func (b *bow) DropNil(nilCols ...string) (Bow, error) {
	// default = all columns
	if len(nilCols) == 0 {
		for _, field := range b.Schema().Fields() {
			nilCols = append(nilCols, field.Name)
		}
	} else {
		nilCols = dedupStrings(nilCols)
	}

	nilColIndexes := make([]int, len(nilCols))
	for i := 0; i < len(nilCols); i++ {
		var err error
		nilColIndexes[i], err = b.GetIndex(nilCols[i])
		if err != nil {
			return nil, err
		}
	}

	var dropped []int
	for ri := 0; ri < b.NumRows(); ri++ {
		for _, ci := range nilColIndexes {
			if b.GetValue(ci, ri) == nil {
				dropped = append(dropped, ri)
				break
			}
		}
	}

	if len(dropped) == 0 {
		return b, nil
	}

	slices := make([]Bow, len(dropped)+1)
	var curr int
	for i, di := range dropped {
		slices[i] = b.NewSlice(curr, di)
		curr = di + 1
	}
	slices[len(dropped)] = b.NewSlice(curr, b.NumRows())

	return AppendBows(slices...)
}

func dedupStrings(s []string) []string {
	seen := make(map[string]struct{}, len(s))
	writeIndex := 0
	for _, v := range s {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		s[writeIndex] = v
		writeIndex++
	}
	return s[:writeIndex]
}

func (b *bow) String() string {
	if b.Record == nil {
		return ""
	}
	w := new(tabwriter.Writer)
	writer := new(strings.Builder)
	// tabs will be replaced by two spaces by formatter
	w.Init(writer, 0, 4, 2, ' ', 0)

	// format any line (header or row)
	formatRow := func(getCellStr func(colIndex int) string) {
		var cells []string
		for colIndex := 0; colIndex < b.NumCols(); colIndex++ {
			cells = append(cells, fmt.Sprintf("%v", getCellStr(colIndex)))
		}
		_, err := fmt.Fprintln(w, strings.Join(cells, "\t"))
		if err != nil {
			panic(err)
		}
	}

	// Print col names on buffer
	formatRow(func(colIndex int) string {
		return fmt.Sprintf("%s:%v", b.Schema().Field(colIndex).Name, b.GetType(colIndex))
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

	for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
		mapChan <- b.GetRow(rowIndex)
	}
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
	for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
		for _, rValIndex := range b.getRightBowIndexesAtRow(b2, commonColumns, rowIndex) {
			for colIndex := range b.Schema().Fields() {
				resultInterfaces[colIndex] = append(resultInterfaces[colIndex], b.GetValue(colIndex, rowIndex))
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

func (b *bow) makeColNamesAndTypesOnJoin(
	b2 *bow, commonColumns map[string]struct{}, rColNotInLIndexes []int) ([]string, []Type) {
	colNames := make([]string, len(b.Schema().Fields())+len(rColNotInLIndexes))
	colType := make([]Type, len(b.Schema().Fields())+len(rColNotInLIndexes))
	for i, f := range b.Schema().Fields() {
		colNames[i] = f.Name
		colType[i] = b.GetType(i)
	}
	for i, index := range rColNotInLIndexes {
		colNames[len(b.Schema().Fields())+i] = b2.Schema().Field(index).Name
		colType[len(b.Schema().Fields())+i] = b2.GetType(index)
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
		for len(i1) == 0 && ok1 {
			i1, ok1 = <-b1Chan
		}
		for len(i2) == 0 && ok2 {
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

func (b *bow) NewSlice(i, j int) Bow {
	return &bow{
		Record: b.Record.NewSlice(int64(i), int64(j)),
	}
}

func (b *bow) NumRows() int {
	if b.Record == nil {
		return 0
	}
	return int(b.Record.NumRows())
}

func (b *bow) NumCols() int {
	if b.Record == nil {
		return 0
	}
	return int(b.Record.NumCols())
}
