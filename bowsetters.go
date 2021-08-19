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
