package bow

import (
	"fmt"
)

func (b *bow) RenameCol(colIndex int, newName string) (Bow, error) {
	if colIndex >= b.NumCols() {
		return nil, fmt.Errorf("bow.RenameCol: column index out of bound")
	}

	if newName == "" {
		return nil, fmt.Errorf("bow.RenameCol: newName cannot be empty")
	}

	seriesSlice := make([]Series, b.NumCols())
	for i, col := range b.Columns() {
		if i == colIndex {
			seriesSlice[i] = Series{
				Name:  newName,
				Array: col,
			}
		} else {
			seriesSlice[i] = b.NewSeriesFromCol(i)
		}
	}

	return NewBowWithMetadata(b.Metadata(), seriesSlice...)
}

// Apply uses the given function to transform a column into something else,
// its expected return type has to be supported otherwise given results will be stored as null
func (b *bow) Apply(colIndex int, returnType Type, fn func(interface{}) interface{}) (Bow, error) {
	buf := NewBuffer(b.NumRows(), returnType)
	for i := 0; i < b.NumRows(); i++ {
		buf.SetOrDropStrict(i, fn(b.GetValue(colIndex, i)))
	}

	seriesSlice := make([]Series, b.NumCols())
	for i := range b.Columns() {
		if i == colIndex {
			seriesSlice[i] = NewSeriesFromBuffer(b.ColumnName(colIndex), buf)
		} else {
			seriesSlice[i] = b.NewSeriesFromCol(i)
		}
	}

	return NewBowWithMetadata(b.Metadata(), seriesSlice...)
}

// Convert transform a column type into another,
// if default behavior is not the one expected, you can use Apply with any implementation needed
func (b *bow) Convert(colIndex int, t Type) (Bow, error) {
	return b.Apply(colIndex, t, t.Convert)
}

// RowCmp implementation is required for Filter
// passing full dataset multidimensional comparators implementations, cross column for instance
// index argument is the current row to compare
type RowCmp func(b Bow, i int) bool

// Filter only preserve row where all given comparators return true
// Filter with no argument return the original bow without copy, but it's not recommended,
// If all filters result in concomitant rows, Filter is as optimal as Slicing in terms of copying
func (b *bow) Filter(fns ...RowCmp) Bow {
	var indices []int
	for i := 0; i < b.NumRows(); i++ {
		if matchRowComps(b, i, fns...) {
			indices = append(indices, i)
		}
	}

	if len(indices) == 0 {
		return b.NewEmptySlice()
	}
	// if all indices are concomitant, slicing is more performent than copying
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

func matchRowComps(b Bow, i int, fns ...RowCmp) bool {
	for _, fn := range fns {
		if !fn(b, i) {
			return false
		}
	}
	return true
}

// MakeFilterValues prepares a valid comparator for Filter, it is lazy on given type
// Be careful about number to string though, for instance 0.1 give "0.100000", which could be unexpected
// If value is of the wrong type and not convertible to column type, comparison will be done on null values!
func (b *bow) MakeFilterValues(colIndex int, values ...interface{}) RowCmp {
	t := b.ColumnType(colIndex)
	for i := range values {
		values[i] = t.Convert(values[i])
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
