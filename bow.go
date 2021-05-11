package bow

import (
	"errors"
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"reflect"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
)

// Bow is a wrapper of Apache Arrow array.Record interface.
// It was not implemented as a facade shadowing Arrow
// in order to expose low level Arrow decisions to Bow users
// while Arrow is in beta.
type Bow interface {

	// Implements Stringer interface
	String() string

	// Getters
	GetType(colIndex int) (colType Type)
	GetName(colIndex int) (colName string, err error)
	GetColumnIndex(colName string) (colIndex int, err error)

	GetRow(rowIndex int) (row map[string]interface{})

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

	// Setters
	SetColName(colIndex int, newName string) (Bow, error)

	// Iterators
	RowMapIter() (rows chan map[string]interface{})

	// Joins
	InnerJoin(other Bow) Bow
	OuterJoin(other Bow) Bow

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

	// Exposed from arrow.Record
	Release()
	Retain()
	Schema() *arrow.Schema
	Column(i int) array.Interface
	NumRows() int
	NumCols() int

	IsColEmpty(colIndex int) bool
	IsColSorted(colIndex int) bool
	IsEmpty() bool
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
	var fields []arrow.Field
	var arrays []array.Interface
	var nRows int64

	if len(series) != 0 && series[0].Array != nil {
		nRows = int64(series[0].Array.Len())
	}

	for _, s := range series {
		if s.Array == nil {
			return nil, errors.New("bow: NewBow: empty Series")
		}
		if s.Name == "" {
			return nil, errors.New("bow: empty Series name")
		}
		if getTypeFromArrowType(s.Array.DataType()) == Unknown {
			return nil, fmt.Errorf(
				"bow: unsupported type: %s",
				s.Array.DataType().Name())
		}
		if int64(s.Array.Len()) != nRows {
			return nil,
				fmt.Errorf(
					"bow: Series '%s' has a length of %d, which is different from the previous ones",
					s.Name, s.Array.Len())
		}
		fields = append(fields, arrow.Field{Name: s.Name, Type: s.Array.DataType()})
		arrays = append(arrays, s.Array)
	}

	schema := arrow.NewSchema(fields, nil)

	return &bow{Record: array.NewRecord(schema, arrays, nRows)}, nil
}

// NewBowFromColBasedInterfaces returns a new Bow with:
// - colNames contains the bow.Record fields names
// - colTypes contains the bow.Record fields data types, and is not mandatory.
//	 If nil, the types will be automatically seeked.
// - colData contains the data to be store in bow.Record
// - colNames and colData need to be of the same size
func NewBowFromColBasedInterfaces(colNames []string, colTypes []Type, colData [][]interface{}) (Bow, error) {
	if len(colNames) != len(colData) {
		return nil, errors.New("bow: colNames and colData array lengths don't match")
	}

	if colTypes != nil && len(colNames) != len(colTypes) {
		return nil, errors.New("bow: colNames and colTypes array lengths don't match")
	} else if colTypes == nil {
		colTypes = make([]Type, len(colNames))
	}

	var err error
	series := make([]Series, len(colNames))
	for i, name := range colNames {
		series[i], err = NewSeriesFromInterfaces(name, colTypes[i], colData[i])
		if err != nil {
			return nil, err
		}
	}
	return NewBow(series...)
}

// NewBowFromRowBasedInterfaces returns a new bow from rowData
// TODO: improve performance of this function
func NewBowFromRowBasedInterfaces(colNames []string, colTypes []Type, rowData [][]interface{}) (Bow, error) {
	columnBasedRows := make([][]interface{}, len(colNames))
	for column := range colNames {
		columnBasedRows[column] = make([]interface{}, len(rowData))
	}
	for rowI, row := range rowData {
		if len(colNames) < len(row) {
			return nil, errors.New("bow: mismatch between columnsNames names and row len")
		}
		for colI := range colNames {
			columnBasedRows[colI][rowI] = row[colI]
		}
	}
	return NewBowFromColBasedInterfaces(colNames, colTypes, columnBasedRows)
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
	var err error
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
	var name string
	for ci := 0; ci < refBow.NumCols(); ci++ {
		var rowOffset int
		typ := refBow.GetType(ci)
		name, err = refBow.GetName(ci)
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

	nilColIndexes := make([]int, len(colNames))
	for i := 0; i < len(colNames); i++ {
		var err error
		nilColIndexes[i], err = b.GetColumnIndex(colNames[i])
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
		slices[i] = b.Slice(curr, di)
		curr = di + 1
	}
	slices[len(dropped)] = b.Slice(curr, b.NumRows())

	return AppendBows(slices...)
}

// SortByCol returns a new Bow with the rows sorted by a column in ascending order.
// The only type currently supported for the column to sort by is Int64
// Returns the same Bow if the column is already sorted
func (b *bow) SortByCol(colName string) (Bow, error) {
	if b.NumCols() == 0 {
		return nil, fmt.Errorf("bow: function SortByCol: empty bow")
	}

	colIndex, err := b.GetColumnIndex(colName)
	if err != nil {
		return nil, fmt.Errorf("bow: function SortByCol: column to sort by not found")
	}

	if b.IsEmpty() {
		return b, nil
	}

	var colToSortBy Int64Col
	var newArray array.Interface
	prevData := b.Record.Column(colIndex).Data()
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	switch b.GetType(colIndex) {
	case Int64:
		// Build the Int64Col interface to store the row indices before sorting
		colToSortBy.Indices = func() []int {
			res := make([]int, b.NumRows())
			for i := range res {
				res[i] = i
			}
			return res
		}()
		prevValues := array.NewInt64Data(prevData)
		colToSortBy.Values = prevValues.Int64Values()
		colToSortBy.Valids = getValids(prevValues, b.NumRows())

		// Stop if sort by column is already sorted
		if Int64ColIsSorted(colToSortBy) {
			return b, nil
		}

		// Sort the column by ascending values
		sort.Sort(colToSortBy)

		builder := array.NewInt64Builder(pool)
		builder.AppendValues(colToSortBy.Values, colToSortBy.Valids)
		newArray = builder.NewArray()
	default:
		return nil, fmt.Errorf("bow: function SortByCol: unsupported type for the column to sort by (Int64 only)")
	}

	// Fill the sort by column with sorted values
	sortedSeries := make([]Series, b.NumCols())
	sortedSeries[colIndex] = Series{
		Name:  colName,
		Array: newArray,
	}

	// Reflect row order changes to fill the other columns
	var wg sync.WaitGroup
	for colIndex, col := range b.Schema().Fields() {
		if col.Name == colName {
			continue
		}
		wg.Add(1)
		go func(colIndex int, col arrow.Field, wg *sync.WaitGroup) {
			defer wg.Done()
			var newArray array.Interface
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			newValids := make([]bool, b.NumRows())
			prevData := b.Record.Column(colIndex).Data()
			switch b.GetType(colIndex) {
			case Int64:
				prevValues := array.NewInt64Data(prevData)
				newValues := make([]int64, b.NumRows())
				for i := 0; i < b.NumRows(); i++ {
					newValues[i] = prevValues.Value(colToSortBy.Indices[i])
					if prevValues.IsValid(colToSortBy.Indices[i]) {
						newValids[i] = true
					}
				}
				builder := array.NewInt64Builder(pool)
				builder.AppendValues(newValues, newValids)
				newArray = builder.NewArray()
			case Float64:
				prevValues := array.NewFloat64Data(prevData)
				newValues := make([]float64, b.NumRows())
				for i := 0; i < b.NumRows(); i++ {
					newValues[i] = prevValues.Value(colToSortBy.Indices[i])
					if prevValues.IsValid(colToSortBy.Indices[i]) {
						newValids[i] = true
					}
				}
				builder := array.NewFloat64Builder(pool)
				builder.AppendValues(newValues, newValids)
				newArray = builder.NewArray()
			case Bool:
				prevValues := array.NewBooleanData(prevData)
				newValues := make([]bool, b.NumRows())
				for i := 0; i < b.NumRows(); i++ {
					newValues[i] = prevValues.Value(colToSortBy.Indices[i])
					if prevValues.IsValid(colToSortBy.Indices[i]) {
						newValids[i] = true
					}
				}
				builder := array.NewBooleanBuilder(pool)
				builder.AppendValues(newValues, newValids)
				newArray = builder.NewArray()
			case String:
				prevValues := array.NewStringData(prevData)
				newValues := make([]string, b.NumRows())
				for i := 0; i < b.NumRows(); i++ {
					newValues[i] = prevValues.Value(colToSortBy.Indices[i])
					if prevValues.IsValid(colToSortBy.Indices[i]) {
						newValids[i] = true
					}
				}
				builder := array.NewStringBuilder(pool)
				builder.AppendValues(newValues, newValids)
				newArray = builder.NewArray()
			default:
				panic(fmt.Sprintf("bow: SortByCol function: unhandled type %s",
					b.Schema().Field(colIndex).Type.Name()))
			}
			sortedSeries[colIndex] = Series{
				Name:  col.Name,
				Array: newArray,
			}
		}(colIndex, col, &wg)
	}
	wg.Wait()
	return NewBow(sortedSeries...)
}

// Int64ColIsSorted tests whether a column of int64s is sorted in increasing order.
func Int64ColIsSorted(col Int64Col) bool { return sort.IsSorted(col) }

// Int64Col implements the methods of sort.Interface, sorting in increasing order
// (not-a-number values are treated as less than other values).
type Int64Col struct {
	Values  []int64
	Valids  []bool
	Indices []int
}

func (p Int64Col) Len() int           { return len(p.Indices) }
func (p Int64Col) Less(i, j int) bool { return p.Values[i] < p.Values[j] }
func (p Int64Col) Swap(i, j int) {
	p.Values[i], p.Values[j] = p.Values[j], p.Values[i]
	p.Valids[i], p.Valids[j] = p.Valids[j], p.Valids[i]
	p.Indices[i], p.Indices[j] = p.Indices[j], p.Indices[i]
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
	if b.NumCols() == 0 {
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

func (b *bow) Slice(i, j int) Bow {
	return &bow{
		Record: b.Record.NewSlice(int64(i), int64(j)),
	}
}

func (b *bow) Select(colNames ...string) (Bow, error) {
	if len(colNames) == 0 {
		return NewBow()
	}

	colsToInclude, err := selectCols(b, colNames)
	if err != nil {
		return nil, err
	}

	var newSeries []Series
	for colIndex, col := range b.Schema().Fields() {
		if colsToInclude[colIndex] {
			newSeries = append(newSeries, Series{
				Name:  col.Name,
				Array: b.Record.Column(colIndex),
			})
		}
	}
	return NewBow(newSeries...)
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
