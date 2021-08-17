package bow

import "fmt"

func (b *bow) NewColName(colIndex int, newName string) (Bow, error) {
	if colIndex >= b.NumCols() {
		return nil, fmt.Errorf("bow: SetColName: column index out of bound")
	}

	if newName == "" {
		return nil, fmt.Errorf("bow: SetColName: newName cannot be empty")
	}

	newSeries := make([]Series, b.NumCols())
	for i, col := range b.Columns() {
		if i == colIndex {
			newSeries[i] = Series{
				Name:  newName,
				Array: col,
			}
		} else {
			oldName := b.ColumnName(i)
			newSeries[i] = Series{
				Name:  oldName,
				Array: col,
			}
		}
	}

	return NewBowWithMetadata(
		Metadata{b.Schema().Metadata()},
		newSeries...)
}
