package bow

import (
	"fmt"
	"sync"
)

// Diff calculates the first discrete difference of each row compared with the previous row.
// If any of the current or the previous row is nil, the result will be nil.
// For boolean columns, XOR operation is used.
// TODO: directly mutate bow && only read currVal at each iteration for performance improvement
func (b *bow) Diff(colNames ...string) (Bow, error) {
	selectedCols, err := selectCols(b, colNames)
	if err != nil {
		return nil, fmt.Errorf("bow.Diff: %w", err)
	}

	for colIndex, col := range b.Schema().Fields() {
		switch b.ColumnType(colIndex) {
		case Int64:
		case Float64:
		case Boolean:
		default:
			return nil, fmt.Errorf(
				"bow.Diff: column '%s' is of unsupported type '%v'",
				col.Name, b.ColumnType(colIndex))
		}
	}

	var wg sync.WaitGroup
	calcSeries := make([]Series, b.NumCols())
	for colIndex, col := range b.Schema().Fields() {
		if !selectedCols[colIndex] {
			calcSeries[colIndex] = b.NewSeriesFromCol(colIndex)
			continue
		}

		wg.Add(1)
		go func(colIndex int, colName string) {
			defer wg.Done()
			colType := b.ColumnType(colIndex)
			colBuf := b.NewBufferFromCol(colIndex)
			calcBuf := NewBuffer(b.NumRows(), colType)
			for rowIndex := 1; rowIndex < b.NumRows(); rowIndex++ {
				valid := b.Column(colIndex).IsValid(rowIndex) &&
					b.Column(colIndex).IsValid(rowIndex-1)
				if !valid {
					continue
				}
				switch colType {
				case Int64:
					currVal := colBuf.GetValue(rowIndex).(int64)
					prevVal := colBuf.GetValue(rowIndex - 1).(int64)
					calcBuf.SetOrDrop(rowIndex, currVal-prevVal)
				case Float64:
					currVal := colBuf.GetValue(rowIndex).(float64)
					prevVal := colBuf.GetValue(rowIndex - 1).(float64)
					calcBuf.SetOrDrop(rowIndex, currVal-prevVal)
				case Boolean:
					currVal := colBuf.GetValue(rowIndex).(bool)
					prevVal := colBuf.GetValue(rowIndex - 1).(bool)
					calcBuf.SetOrDrop(rowIndex, currVal != prevVal)
				}
			}
			calcSeries[colIndex] = NewSeriesFromBuffer(colName, calcBuf)

		}(colIndex, col.Name)
	}
	wg.Wait()

	return NewBowWithMetadata(b.Metadata(), calcSeries...)
}
