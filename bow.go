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

	// Implements Stringer interface
	String() string

	// Getters
	GetColType(colIndex int) Type
	GetColName(colIndex int) string
	GetColIndices(colName string) []int

	GetRow(rowIndex int) (row map[string]interface{})

	NewBufferFromCol(colIndex int) Buffer
	NewSeriesFromCol(colIndex int) Series
	GetValueByName(colName string, rowIndex int) (value interface{})
	GetValue(colIndex, rowIndex int) (value interface{})
	GetNextValue(colIndex, rowIndex int) (value interface{}, resRowIndex int)
	GetNextValues(colIndex1, colIndex2, rowIndex int) (value1, value2 interface{}, resRowIndex int)
	GetPreviousValue(colIndex, rowIndex int) (value interface{}, resultRowIndex int)
	GetPreviousValues(colIndex1, colIndex2, rowIndex int) (value1, value2 interface{}, resRowIndex int)

	GetInt64(colIndex, rowIndex int) (value int64, valid bool)
	GetNextInt64(colIndex, rowIndex int) (value int64, resRowIndex int)
	GetPreviousInt64(colIndex, rowIndex int) (value int64, resRowIndex int)

	GetFloat64(colIndex, rowIndex int) (value float64, valid bool)
	GetNextFloat64(colIndex, rowIndex int) (value float64, resRowIndex int)
	GetNextFloat64s(colIndex1, colIndex2, rowIndex int) (value1, value2 float64, resRowIndex int)
	GetPreviousFloat64(colIndex, rowIndex int) (value float64, resRowIndex int)
	GetPreviousFloat64s(colIndex1, colIndex2, rowIndex int) (value1, value2 float64, resRowIndex int)

	FindFirst(colIndex int, value interface{}) (rowIndex int)

	GetMetadata() Metadata
	SetMetadata(key, value string) Bow

	// Setters
	SetColName(colIndex int, newName string) (Bow, error)

	// Iterators
	RowMapIter() (rows chan map[string]interface{})

	// Joins
	InnerJoin(other Bow) Bow
	OuterJoin(other Bow) Bow

	// Calculations
	Diff(colNames ...string) (Bow, error)

	Equal(other Bow) (equal bool)

	MarshalJSON() (buf []byte, err error)
	UnmarshalJSON(data []byte) error
	NewValuesFromJSON(jsonB JSONBow) error

	Slice(i, j int) Bow
	Select(colNames ...string) (Bow, error)
	NewEmpty() Bow
	DropNil(colNames ...string) (Bow, error)
	SortByCol(colName string) (Bow, error)

	// Missing data handling
	FillPrevious(colNames ...string) (Bow, error)
	FillNext(colNames ...string) (Bow, error)
	FillMean(colNames ...string) (Bow, error)
	FillLinear(refColName, toFillColName string) (Bow, error)

	// Parquet file format
	WriteParquet(path string, verbose bool) error

	// Exposed from arrow.Record
	Schema() *arrow.Schema
	Column(colIndex int) array.Interface
	NumRows() int
	NumCols() int

	IsColEmpty(colIndex int) bool
	IsColSorted(colIndex int) bool
	IsEmpty() bool

	AddCols(series ...Series) (Bow, error)
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

func (b *bow) NewEmpty() Bow {
	return b.Slice(0, 0)
}

// DropNil drops any row that contains a nil for any of `colNames`.
// `colNames` defaults to all columns.
func (b *bow) DropNil(colNames ...string) (Bow, error) {
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
		colIndices := b.GetColIndices(colNames[colIndex])
		if len(colIndices) == 0 {
			return nil, fmt.Errorf("bow.DropNil: column %q does not exist", colNames[colIndex])
		} else if len(colIndices) > 1 {
			return nil, fmt.Errorf("bow.DropNil: several columns %q found", colNames[colIndex])
		}
		nilColIndices[colIndex] = colIndices[0]
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

func (b *bow) RowMapIter() chan map[string]interface{} {
	rows := make(chan map[string]interface{})
	go b.rowMapIter(rows)

	return rows
}

func (b *bow) rowMapIter(rows chan map[string]interface{}) {
	defer close(rows)

	if b.Record == nil || b.NumRows() <= 0 {
		return
	}

	for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
		rows <- b.GetRow(rowIndex)
	}
}

func (b *bow) Equal(other Bow) bool {
	b2, ok := other.(*bow)
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

	if b.Schema().Metadata().String() != b2.Schema().Metadata().String() {
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

func (b *bow) Slice(i, j int) Bow {
	return &bow{
		Record: b.Record.NewSlice(int64(i), int64(j)),
	}
}

func (b *bow) Select(colNames ...string) (Bow, error) {
	if len(colNames) == 0 {
		return NewBowWithMetadata(
			Metadata{b.Schema().Metadata()})
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

func (b *bow) AddCols(series ...Series) (Bow, error) {
	if len(series) == 0 {
		return b, nil
	}

	addedColNames := make(map[string]*interface{}, b.NumCols()+len(series))
	seriesSlice := make([]Series, b.NumCols()+len(series))

	for colIndex, col := range b.Schema().Fields() {
		seriesSlice[colIndex] = b.NewSeriesFromCol(colIndex)
		addedColNames[col.Name] = nil
	}

	for i, s := range series {
		_, ok := addedColNames[s.Name]
		if ok {
			return nil, fmt.Errorf("bow.AddCols: column %q already exists", s.Name)
		}
		seriesSlice[b.NumCols()+i] = s
		addedColNames[s.Name] = nil
	}
	return NewBowWithMetadata(b.GetMetadata(), seriesSlice...)
}

func getValid(arr array.Interface, length int) []bool {
	valid := make([]bool, length)

	for i := 0; i < length; i++ {
		valid[i] = arr.IsValid(i)
	}
	return valid
}
