package bow

import (
	"fmt"
	"sync"
)

// Diff calculates the first discrete difference of each row compared with the previous row.
// For boolean columns, XOR operation is used.
func (b *bow) Diff(colNames ...string) (Bow, error) {
	selectedCols, err := selectCols(b, colNames)
	if err != nil {
		return nil, fmt.Errorf(
			"bow.Diff error selecting columns [%s] on bow schema [%s]: %w",
			colNames, b.Schema().String(), err)
	}

	for colIndex, col := range b.Schema().Fields() {
		switch b.GetType(colIndex) {
		case Int64:
		case Float64:
		case Bool:
		default:
			return nil, fmt.Errorf(
				"bow.Diff type error: column '%s' is of type '%s'",
				col.Name, b.GetType(colIndex))
		}
	}

	var wg sync.WaitGroup
	calcSeries := make([]Series, b.NumCols())
	for colIndex, col := range b.Schema().Fields() {
		wg.Add(1)
		go func(colIndex int, colName string, wg *sync.WaitGroup) {
			defer wg.Done()

			typ := b.GetType(colIndex)
			if selectedCols[colIndex] {
				buf := NewBuffer(b.NumRows(), typ, true)
				for rowIndex := 1; rowIndex < b.NumRows(); rowIndex++ {
					valid := b.Column(colIndex).IsValid(rowIndex) && b.Column(colIndex).IsValid(rowIndex-1)
					if valid {
						switch typ {
						case Int64:
							currVal, _ := b.GetInt64(colIndex, rowIndex)
							prevVal, _ := b.GetInt64(colIndex, rowIndex-1)
							buf.SetOrDrop(rowIndex, currVal-prevVal)
						case Float64:
							currVal, _ := b.GetFloat64(colIndex, rowIndex)
							prevVal, _ := b.GetFloat64(colIndex, rowIndex-1)
							buf.SetOrDrop(rowIndex, currVal-prevVal)
						case Bool:
							currVal, _ := b.GetInt64(colIndex, rowIndex)
							prevVal, _ := b.GetInt64(colIndex, rowIndex-1)
							buf.SetOrDrop(rowIndex, currVal != prevVal)
						}
					}
				}

				calcSeries[colIndex] = NewSeries(colName, typ, buf.Value, buf.Valid)
			} else {
				calcSeries[colIndex] = Series{
					Name:  colName,
					Array: b.Record.Column(colIndex),
				}
			}
		}(colIndex, col.Name, &wg)
	}
	wg.Wait()

	return NewBow(calcSeries...)
}
