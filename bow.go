package bow

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

// Bow is a wrapper of Apache Arrow array.Record interface.
// It was not implemented as a facade shadowing Arrow
// in order to expose low level Arrow decisions to Bow users
// while Arrow is in beta.
type Bow interface {
	String() string
	Schema() *arrow.Schema
	Column(colIndex int) array.Interface
	ColumnName(colIndex int) string
	NumRows() int
	NumCols() int

	ColumnType(colIndex int) Type
	ColumnIndex(colName string) (int, error)
	NewBufferFromCol(colIndex int) Buffer
	NewSeriesFromCol(colIndex int) Series
	Metadata() Metadata
	SetMetadata(key, value string) Bow

	GetRow(rowIndex int) map[string]interface{}
	GetRowsChan() chan map[string]interface{}

	GetValueByName(colName string, rowIndex int) interface{}
	GetValue(colIndex, rowIndex int) interface{}
	GetNextValue(colIndex, rowIndex int) (value interface{}, resRowIndex int)
	GetNextValues(colIndex1, colIndex2, rowIndex int) (value1, value2 interface{}, resRowIndex int)
	GetNextRowIndex(colIndex, rowIndex int) int
	GetPreviousValue(colIndex, rowIndex int) (value interface{}, resRowIndex int)
	GetPreviousValues(colIndex1, colIndex2, rowIndex int) (value1, value2 interface{}, resRowIndex int)
	GetPreviousRowIndex(colIndex, rowIndex int) int

	GetInt64(colIndex, rowIndex int) (value int64, valid bool)
	GetNextInt64(colIndex, rowIndex int) (value int64, resRowIndex int)
	GetPreviousInt64(colIndex, rowIndex int) (value int64, resRowIndex int)

	GetFloat64(colIndex, rowIndex int) (value float64, valid bool)
	GetNextFloat64(colIndex, rowIndex int) (value float64, resRowIndex int)
	GetNextFloat64s(colIndex1, colIndex2, rowIndex int) (value1, value2 float64, resRowIndex int)
	GetPreviousFloat64(colIndex, rowIndex int) (value float64, resRowIndex int)
	GetPreviousFloat64s(colIndex1, colIndex2, rowIndex int) (value1, value2 float64, resRowIndex int)

	AddCols(newCols ...Series) (Bow, error)
	NewColName(colIndex int, newName string) (Bow, error)

	InnerJoin(other Bow) Bow
	OuterJoin(other Bow) Bow

	Diff(colNames ...string) (Bow, error)

	Slice(i, j int) Bow
	Select(colNames ...string) (Bow, error)
	ClearRows() Bow
	DropNils(colNames ...string) (Bow, error)
	SortByCol(colName string) (Bow, error)

	FillPrevious(colNames ...string) (Bow, error)
	FillNext(colNames ...string) (Bow, error)
	FillMean(colNames ...string) (Bow, error)
	FillLinear(refColName, toFillColName string) (Bow, error)

	Equal(other Bow) bool
	IsColEmpty(colIndex int) bool
	IsColSorted(colIndex int) bool

	MarshalJSON() (buf []byte, err error)
	UnmarshalJSON(data []byte) error
	NewValuesFromJSON(jsonB JSONBow) error
	WriteParquet(path string, verbose bool) error
}

type bow struct {
	array.Record
}

func NewBowEmpty() Bow {
	var fields []arrow.Field
	var arrays []array.Interface
	schema := arrow.NewSchema(fields, nil)
	return &bow{Record: array.NewRecord(schema, arrays, 0)}
}

func NewBow(series ...Series) (Bow, error) {
	rec, err := newRecord(Metadata{}, series...)
	if err != nil {
		return nil, fmt.Errorf("bow.NewBow: %w", err)
	}

	return &bow{Record: rec}, nil
}

// NewBowFromColBasedInterfaces returns a new Bow with:
// - colNames containing the bow.Record fields names
// - colTypes containing the bow.Record fields data types, and is not mandatory.
//	 If nil, the types will be automatically seeked.
// - colData containing the data to be store in bow.Record
// - colNames and colData need to be of the same size
func NewBowFromColBasedInterfaces(colNames []string, colTypes []Type, colData [][]interface{}) (Bow, error) {
	if len(colNames) != len(colData) {
		return nil, errors.New("bow.NewBowFromColBasedInterfaces: colNames and colData array lengths don't match")
	}

	if colTypes == nil {
		colTypes = make([]Type, len(colNames))
	} else if len(colNames) != len(colTypes) {
		return nil, errors.New("bow.NewBowFromColBasedInterfaces: colNames and colTypes array lengths don't match")
	}

	var err error
	seriesSlice := make([]Series, len(colNames))
	for i, colName := range colNames {
		seriesSlice[i], err = NewSeriesFromInterfaces(colName, colTypes[i], colData[i])
		if err != nil {
			return nil, err
		}
	}

	return NewBow(seriesSlice...)
}

// NewBowFromRowBasedInterfaces returns a new bow from row based data
func NewBowFromRowBasedInterfaces(colNames []string, colTypes []Type, rowBasedData [][]interface{}) (Bow, error) {
	if len(colNames) != len(colTypes) {
		return nil, errors.New(
			"bow.NewBowFromRowBasedInterfaces: mismatch between colNames and colTypes len")
	}

	bufSlice := make([]Buffer, len(colNames))
	for i := range bufSlice {
		bufSlice[i] = NewBuffer(len(rowBasedData), colTypes[i], true)
	}

	for rowIndex, row := range rowBasedData {
		if len(row) != len(colNames) {
			return nil, errors.New(
				"bow.NewBowFromRowBasedInterfaces: mismatch between colNames and row len")
		}
		for colIndex := range colNames {
			bufSlice[colIndex].SetOrDrop(rowIndex, row[colIndex])
		}
	}

	seriesSlice := make([]Series, len(colNames))
	for i := range colNames {
		seriesSlice[i] = NewSeries(colNames[i], colTypes[i], bufSlice[i].Value, bufSlice[i].Valid)
	}

	return NewBow(seriesSlice...)
}

func (b *bow) ClearRows() Bow {
	return b.Slice(0, 0)
}

// DropNils drops any row that contains a nil for any of `colNames`.
// `colNames` defaults to all columns.
func (b *bow) DropNils(colNames ...string) (Bow, error) {
	// default = all columns
	if len(colNames) == 0 {
		for _, field := range b.Schema().Fields() {
			colNames = append(colNames, field.Name)
		}
	} else {
		colNames = dedupStrings(colNames)
	}

	nilColIndices := make([]int, len(colNames))
	for colIndex := range colNames {
		var err error
		nilColIndices[colIndex], err = b.ColumnIndex(colNames[colIndex])
		if err != nil {
			return nil, fmt.Errorf("bow.DropNils: %w", err)
		}
	}

	var droppedRowIndices []int
	for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
		for _, nilColIndex := range nilColIndices {
			if b.GetValue(nilColIndex, rowIndex) == nil {
				droppedRowIndices = append(droppedRowIndices, rowIndex)
				break
			}
		}
	}

	if len(droppedRowIndices) == 0 {
		return b, nil
	}

	bowSlice := make([]Bow, len(droppedRowIndices)+1)
	var curr int
	for i, droppedRowIndex := range droppedRowIndices {
		bowSlice[i] = b.Slice(curr, droppedRowIndex)
		curr = droppedRowIndex + 1
	}
	bowSlice[len(droppedRowIndices)] = b.Slice(curr, b.NumRows())

	return AppendBows(bowSlice...)
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

func (b *bow) GetRowsChan() chan map[string]interface{} {
	rows := make(chan map[string]interface{})
	go b.getRowsChan(rows)

	return rows
}

func (b *bow) getRowsChan(rows chan map[string]interface{}) {
	defer close(rows)

	if b.Record == nil || b.NumRows() == 0 {
		return
	}

	for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
		rows <- b.GetRow(rowIndex)
	}
}

func (b *bow) Equal(other Bow) bool {
	b2, ok := other.(*bow)
	if !ok {
		panic("bow.Equal: 'other' isn't a bow object")
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

func (b *bow) Slice(i, j int) Bow {
	return &bow{
		Record: b.Record.NewSlice(int64(i), int64(j)),
	}
}

func (b *bow) Select(colNames ...string) (Bow, error) {
	if len(colNames) == 0 {
		return NewBowWithMetadata(
			Metadata{b.Schema().Metadata()},
		)
	}

	selectedCols, err := selectCols(b, colNames)
	if err != nil {
		return nil, err
	}

	var seriesSlice []Series
	for colIndex := range b.Schema().Fields() {
		if selectedCols[colIndex] {
			seriesSlice = append(seriesSlice, b.NewSeriesFromCol(colIndex))
		}
	}

	return NewBowWithMetadata(
		Metadata{b.Schema().Metadata()},
		seriesSlice...)
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

func (b *bow) AddCols(seriesSlice ...Series) (Bow, error) {
	if len(seriesSlice) == 0 {
		return b, nil
	}

	addedColNames := make(map[string]*interface{}, b.NumCols()+len(seriesSlice))
	newSeriesSlice := make([]Series, b.NumCols()+len(seriesSlice))

	for colIndex, col := range b.Schema().Fields() {
		newSeriesSlice[colIndex] = b.NewSeriesFromCol(colIndex)
		addedColNames[col.Name] = nil
	}

	for i, s := range seriesSlice {
		_, ok := addedColNames[s.Name]
		if ok {
			return nil, fmt.Errorf("bow.AddCols: column %q already exists", s.Name)
		}
		newSeriesSlice[b.NumCols()+i] = s
		addedColNames[s.Name] = nil
	}

	return NewBowWithMetadata(b.Metadata(), newSeriesSlice...)
}

func getValid(arr array.Interface, length int) []bool {
	valid := make([]bool, length)

	for i := 0; i < length; i++ {
		valid[i] = arr.IsValid(i)
	}
	return valid
}

func (b *bow) NewSeriesFromCol(colIndex int) Series {
	return Series{
		Name:  b.ColumnName(colIndex),
		Array: b.Column(colIndex),
	}
}
