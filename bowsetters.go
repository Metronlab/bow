package bow

import (
	"fmt"
)

// RenameCol returns a new Bow with the column `colIndex` renamed.
func (b *bow) RenameCol(colIndex int, newName string) (Bow, error) {
	if colIndex >= b.NumCols() {
		return nil, fmt.Errorf("column index out of bound")
	}

	if newName == "" {
		return nil, fmt.Errorf("newName cannot be empty")
	}

	series := make([]Series, b.NumCols())
	for i, col := range b.Columns() {
		if i == colIndex {
			series[i] = Series{
				Name:  newName,
				Array: col,
			}
		} else {
			series[i] = b.NewSeriesFromCol(i)
		}
	}

	return NewBowWithMetadata(b.Metadata(), series...)
}

// Apply uses the given function to transform the values of column `colIndex`.
// Its expected return type has to be supported otherwise given results will be stored as nil values.
func (b *bow) Apply(colIndex int, returnType Type, fn func(interface{}) interface{}) (Bow, error) {
	buf := NewBuffer(b.NumRows(), returnType)
	for i := 0; i < b.NumRows(); i++ {
		buf.SetOrDropStrict(i, fn(b.GetValue(colIndex, i)))
	}

	series := make([]Series, b.NumCols())
	for i := range b.Columns() {
		if i == colIndex {
			series[i] = NewSeriesFromBuffer(b.ColumnName(colIndex), buf)
		} else {
			series[i] = b.NewSeriesFromCol(i)
		}
	}

	return NewBowWithMetadata(b.Metadata(), series...)
}

// Convert transforms a column type into another,
// if default behavior is not the one expected, you can use Apply with any implementation needed
func (b *bow) Convert(colIndex int, t Type) (Bow, error) {
	return b.Apply(colIndex, t, t.Convert)
}

// RowCmp implementation is required for Filter
// passing full dataset multidimensional comparators implementations, cross column for instance
// index argument is the current row to compare
type RowCmp func(b Bow, i int) bool

// Filter only preserves the rows where all given comparators return true
// Filter with no argument return the original bow without copy, but it's not recommended,
// If all filters result in concomitant rows, Filter is as optimal as Slicing in terms of copying
func (b *bow) Filter(fns ...RowCmp) Bow {
	var indices []int
	for i := 0; i < b.NumRows(); i++ {
		if matchRowCmps(b, i, fns...) {
			indices = append(indices, i)
		}
	}

	if len(indices) == 0 {
		return b.NewEmptySlice()
	}

	// If all indices are concomitant, slicing is more performent than copying
	lastInclusive := indices[len(indices)-1] + 1
	if len(indices) == lastInclusive-indices[0] {
		return b.NewSlice(indices[0], lastInclusive)
	}

	filteredSeries := make([]Series, b.NumCols())
	for colIndex := 0; colIndex < b.NumCols(); colIndex++ {
		buf := NewBuffer(len(indices), b.ColumnType(colIndex))
		for i, j := range indices {
			buf.SetOrDropStrict(i, b.GetValue(colIndex, j))
		}
		filteredSeries[colIndex] = NewSeriesFromBuffer(b.ColumnName(colIndex), buf)
	}

	res, err := NewBowWithMetadata(b.Metadata(), filteredSeries...)
	if err != nil {
		panic(err)
	}

	return res
}

func matchRowCmps(b Bow, i int, fns ...RowCmp) bool {
	for _, fn := range fns {
		if !fn(b, i) {
			return false
		}
	}

	return true
}

// MakeFilterValues prepares a valid comparator for Filter, it is lazy on given type.
// Be careful about number to string though, for instance 0.1 give "0.100000", which could be unexpected
// If value is of the wrong type and not convertible to column type, comparison will be done on null values!
func (b *bow) MakeFilterValues(colIndex int, values ...interface{}) RowCmp {
	for i := range values {
		values[i] = b.ColumnType(colIndex).Convert(values[i])
	}

	return func(b Bow, i int) bool {
		return contains(values, b.GetValue(colIndex, i))
	}
}

func contains(values []interface{}, value interface{}) bool {
	for _, val := range values {
		if val == value {
			return true
		}
	}

	return false
}
