package bow

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
)

// Bow is wrapping the Apache Arrow arrow.Record interface,
// which is a collection of equal-length arrow.Array matching a particular arrow.Schema.
// Its purpose is to add convenience methods to easily manipulate dataframes.
type Bow interface {
	String() string
	Schema() *arrow.Schema
	ColumnName(colIndex int) string
	NumRows() int
	NumCols() int

	ColumnType(colIndex int) Type
	ColumnIndex(colName string) (int, error)
	NewBufferFromCol(colIndex int) Buffer
	NewSeriesFromCol(colIndex int) Series

	Metadata() Metadata
	WithMetadata(metadata Metadata) Bow
	SetMetadata(key, value string) Bow

	GetRow(rowIndex int) map[string]interface{}
	GetRowsChan() <-chan map[string]interface{}

	GetValue(colIndex, rowIndex int) interface{}
	GetPrevValue(colIndex, rowIndex int) (value interface{}, resRowIndex int)
	GetNextValue(colIndex, rowIndex int) (value interface{}, resRowIndex int)
	GetPrevValues(colIndex1, colIndex2, rowIndex int) (value1, value2 interface{}, resRowIndex int)
	GetNextValues(colIndex1, colIndex2, rowIndex int) (value1, value2 interface{}, resRowIndex int)
	GetPrevRowIndex(colIndex, rowIndex int) int
	GetNextRowIndex(colIndex, rowIndex int) int

	GetInt64(colIndex, rowIndex int) (value int64, valid bool)
	GetPrevInt64(colIndex, rowIndex int) (value int64, resRowIndex int)
	GetNextInt64(colIndex, rowIndex int) (value int64, resRowIndex int)

	GetFloat64(colIndex, rowIndex int) (value float64, valid bool)
	GetPrevFloat64(colIndex, rowIndex int) (value float64, resRowIndex int)
	GetNextFloat64(colIndex, rowIndex int) (value float64, resRowIndex int)
	GetPrevFloat64s(colIndex1, colIndex2, rowIndex int) (value1, value2 float64, resRowIndex int)
	GetNextFloat64s(colIndex1, colIndex2, rowIndex int) (value1, value2 float64, resRowIndex int)

	Distinct(colIndex int) Bow

	Find(columnIndex int, value interface{}) int
	FindNext(columnIndex, rowIndex int, value interface{}) int
	Contains(columnIndex int, value interface{}) bool

	Filter(fns ...RowCmp) Bow
	MakeFilterValues(colIndex int, values ...interface{}) RowCmp

	AddCols(newCols ...Series) (Bow, error)
	RenameCol(colIndex int, newName string) (Bow, error)
	Apply(colIndex int, returnType Type, fn func(interface{}) interface{}) (Bow, error)
	Convert(colIndex int, t Type) (Bow, error)

	InnerJoin(other Bow) Bow
	OuterJoin(other Bow) Bow

	Diff(colIndices ...int) (Bow, error)

	NewSlice(i, j int) Bow
	Select(colIndices ...int) (Bow, error)
	NewEmptySlice() Bow
	DropNils(colIndices ...int) (Bow, error)
	SortByCol(colIndex int) (Bow, error)

	FillPrevious(colIndices ...int) (Bow, error)
	FillNext(colIndices ...int) (Bow, error)
	FillMean(colIndices ...int) (Bow, error)
	FillLinear(refColIndex, toFillColIndex int) (Bow, error)

	Equal(other Bow) bool
	IsColEmpty(colIndex int) bool
	IsColSorted(colIndex int) bool

	MarshalJSON() (buf []byte, err error)
	UnmarshalJSON(data []byte) error
	NewValuesFromJSON(jsonB JSONBow) error
	WriteParquet(path string, verbose bool) error
	GetParquetMetaColTimeUnit(colIndex int) (time.Duration, error)
}

type bow struct {
	arrow.Record
}

// NewBowEmpty returns a new empty Bow.
func NewBowEmpty() Bow {
	var fields []arrow.Field
	var arrays []arrow.Array
	schema := arrow.NewSchema(fields, nil)
	return &bow{Record: array.NewRecord(schema, arrays, 0)}
}

// NewBow returns a new Bow from one or more Series.
func NewBow(series ...Series) (Bow, error) {
	rec, err := newRecord(Metadata{}, series...)
	if err != nil {
		return nil, fmt.Errorf("newRecord: %w", err)
	}

	return &bow{Record: rec}, nil
}

// NewBowFromColBasedInterfaces returns a new Bow:
// - colNames contains the Series names
// - colTypes contains the Series data types, optional
//   (if nil, the types will be automatically seeked)
// - colBasedData contains the data itself as a two-dimensional slice,
//   with the first dimension being the columns
//   (colNames and colBasedData need to be of the same size)
func NewBowFromColBasedInterfaces(colNames []string, colTypes []Type, colBasedData [][]interface{}) (Bow, error) {
	if len(colNames) != len(colBasedData) {
		return nil, errors.New("colNames and colBasedData slices lengths don't match")
	}

	if colTypes == nil {
		colTypes = make([]Type, len(colNames))
	} else if len(colNames) != len(colTypes) {
		return nil, errors.New("colNames and colTypes slices lengths don't match")
	}

	series := make([]Series, len(colNames))
	for i, colName := range colNames {
		series[i] = NewSeriesFromInterfaces(colName, colTypes[i], colBasedData[i])
	}

	return NewBow(series...)
}

// NewBowFromRowBasedInterfaces returns a new Bow:
// - colNames contains the Series names
// - colTypes contains the Series data types, required
// - rowBasedData contains the data itself as a two-dimensional slice,
//   with the first dimension being the rows
//   (colNames and rowBasedData need to be of the same size)
func NewBowFromRowBasedInterfaces(colNames []string, colTypes []Type, rowBasedData [][]interface{}) (Bow, error) {
	if len(colNames) != len(colTypes) {
		return nil, errors.New("colNames and colTypes slices lengths don't match")
	}

	buffers := make([]Buffer, len(colNames))
	for i := range buffers {
		buffers[i] = NewBuffer(len(rowBasedData), colTypes[i])
	}

	for rowIndex, row := range rowBasedData {
		if len(row) != len(colNames) {
			return nil, errors.New("colNames and row slices lengths don't match")
		}

		for colIndex := range colNames {
			buffers[colIndex].SetOrDrop(rowIndex, row[colIndex])
		}
	}

	series := make([]Series, len(colNames))
	for i := range colNames {
		series[i] = NewSeriesFromBuffer(colNames[i], buffers[i])
	}

	return NewBow(series...)
}

// NewEmptySlice returns an empty slice of the Bow.
func (b *bow) NewEmptySlice() Bow {
	return b.NewSlice(0, 0)
}

// DropNils drops any row that contains a nil for any of `colIndices`.
// `colIndices` defaults to all columns.
func (b *bow) DropNils(colIndices ...int) (Bow, error) {
	selectedCols, err := selectCols(b, colIndices)
	if err != nil {
		return nil, err
	}

	var droppedRowIndices []int
	for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
		for colIndex, selected := range selectedCols {
			if !selected {
				continue
			}

			if b.GetValue(colIndex, rowIndex) == nil {
				droppedRowIndices = append(droppedRowIndices, rowIndex)
				break
			}
		}
	}

	if len(droppedRowIndices) == 0 {
		return b, nil
	}

	bows := make([]Bow, len(droppedRowIndices)+1)
	var curr int
	for i, droppedRowIndex := range droppedRowIndices {
		bows[i] = b.NewSlice(curr, droppedRowIndex)
		curr = droppedRowIndex + 1
	}

	bows[len(droppedRowIndices)] = b.NewSlice(curr, b.NumRows())

	return AppendBows(bows...)
}

// Equal returns true if the two Bow are equal: their Record, Schema and Metadata should be equal.
func (b *bow) Equal(other Bow) bool {
	b2, ok := other.(*bow)
	if !ok {
		panic("'other' isn't a bow object")
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

	if b.Schema().Metadata().String() != b2.Schema().Metadata().String() {
		return false
	}

	b1Chan := b.GetRowsChan()
	b2Chan := b2.GetRowsChan()

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

// NewSlice returns a new Bow with a zero-copy slice of the arrow.Record.
// i and j being the minimum and maximum rows respectively.
func (b *bow) NewSlice(i, j int) Bow {
	return &bow{
		Record: b.Record.NewSlice(int64(i), int64(j)),
	}
}

// Select returns a copy of the Bow, including only the columns from `colIndices`.
func (b *bow) Select(colIndices ...int) (Bow, error) {
	if len(colIndices) == 0 {
		return NewBowWithMetadata(b.Metadata())
	}

	selectedCols, err := selectCols(b, colIndices)
	if err != nil {
		return nil, err
	}

	var series []Series
	for colIndex := range b.Schema().Fields() {
		if selectedCols[colIndex] {
			series = append(series, b.NewSeriesFromCol(colIndex))
		}
	}

	return NewBowWithMetadata(b.Metadata(), series...)
}

// NumRows returns the number of rows in the Bow.
func (b *bow) NumRows() int {
	if b.Record == nil {
		return 0
	}

	return int(b.Record.NumRows())
}

// NumCols returns the number of columns in the Bow.
func (b *bow) NumCols() int {
	if b.Record == nil {
		return 0
	}

	return int(b.Record.NumCols())
}

// AddCols returns a copy of the Bow with extra columns from the `series`.
func (b *bow) AddCols(series ...Series) (Bow, error) {
	addedColNames := make(map[string]*interface{}, b.NumCols()+len(series))
	newSeries := make([]Series, b.NumCols()+len(series))

	for colIndex, col := range b.Schema().Fields() {
		newSeries[colIndex] = b.NewSeriesFromCol(colIndex)
		addedColNames[col.Name] = nil
	}

	for i, s := range series {
		_, ok := addedColNames[s.Name]
		if ok {
			return nil, fmt.Errorf("column %q already exists", s.Name)
		}
		newSeries[b.NumCols()+i] = s
		addedColNames[s.Name] = nil
	}

	return NewBowWithMetadata(b.Metadata(), newSeries...)
}

// NewSeriesFromCol returns a Series from the column `colIndex`.
func (b *bow) NewSeriesFromCol(colIndex int) Series {
	return Series{
		Name:  b.ColumnName(colIndex),
		Array: b.Column(colIndex),
	}
}

func getValiditySlice(arr arrow.Array) []bool {
	valid := make([]bool, arr.Len())

	for i := 0; i < arr.Len(); i++ {
		valid[i] = arr.IsValid(i)
	}

	return valid
}
