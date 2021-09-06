package bow

import "fmt"

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

// Apply use the given function to transform a column into something else,
// it's expect return type to be correct otherwise given results will be stored as null
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
