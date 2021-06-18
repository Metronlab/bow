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

	GetMetadata() Metadata

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
			return nil,
				fmt.Errorf("bow.AppendBow: schema mismatch: got both\n%v\nand\n%v",
					refSchema, schema)
		}

		if schema.Metadata().String() != refSchema.Metadata().String() {
			return nil,
				fmt.Errorf("bow.AppendBow: schema Metadata mismatch: got both\n%v\nand\n%v",
					refSchema.Metadata(), schema.Metadata())
		}

		numRows += b.NumRows()
	}

	seriesSlice := make([]Series, refBow.NumCols())
	bufSlice := make([]Buffer, refBow.NumCols())
	var name string
	for ci := 0; ci < refBow.NumCols(); ci++ {
		var rowOffset int
		typ := refBow.GetType(ci)
		name, err = refBow.GetName(ci)
		if err != nil {
			return nil, err
		}
		bufSlice[ci] = NewBuffer(numRows, typ, true)
		for _, b := range bows {
			for ri := 0; ri < b.NumRows(); ri++ {
				bufSlice[ci].SetOrDrop(ri+rowOffset, b.GetValue(ci, ri))
			}
			rowOffset += b.NumRows()
		}

		seriesSlice[ci] = NewSeries(name, typ, bufSlice[ci].Value, bufSlice[ci].Valid)
	}

	return NewBowWithMetadata(
		Metadata{refSchema.Metadata()},
		seriesSlice...)
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

	return NewBowWithMetadata(
		Metadata{b.Schema().Metadata()},
		newSeries...)
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
	seriesNbr := len(series)
	if seriesNbr == 0 {
		return b, nil
	}

	bowNumCols := b.NumCols()
	addedColNames := make(map[string]*interface{}, bowNumCols+seriesNbr)
	newSeries := make([]Series, bowNumCols+seriesNbr)

	for colIndex, col := range b.Schema().Fields() {
		newSeries[colIndex] = Series{
			Name:  col.Name,
			Array: b.Record.Column(colIndex),
		}
		addedColNames[col.Name] = nil
	}

	for i, s := range series {
		_, ok := addedColNames[s.Name]
		if ok {
			return nil, fmt.Errorf("column %q already exists", s.Name)
		}
		newSeries[bowNumCols+i] = s
		addedColNames[s.Name] = nil
	}
	return NewBow(newSeries...)
}
