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
			colSeries := b.NewSeriesFromCol(colIndex)
			calcSeries[colIndex] = NewSeries(colName, b.NumRows(), colType)
			for rowIndex := 1; rowIndex < b.NumRows(); rowIndex++ {
				valid := b.Column(colIndex).IsValid(rowIndex) &&
					b.Column(colIndex).IsValid(rowIndex-1)
				if !valid {
					continue
				}
				switch colType {
				case Int64:
					currVal := colSeries.GetValue(rowIndex).(int64)
					prevVal := colSeries.GetValue(rowIndex - 1).(int64)
					calcSeries[colIndex].SetOrDrop(rowIndex, currVal-prevVal)
				case Float64:
					currVal := colSeries.GetValue(rowIndex).(float64)
					prevVal := colSeries.GetValue(rowIndex - 1).(float64)
					calcSeries[colIndex].SetOrDrop(rowIndex, currVal-prevVal)
				case Boolean:
					currVal := colSeries.GetValue(rowIndex).(bool)
					prevVal := colSeries.GetValue(rowIndex - 1).(bool)
					calcSeries[colIndex].SetOrDrop(rowIndex, currVal != prevVal)
				}
			}

		}(colIndex, col.Name)
	}
	wg.Wait()

	return NewBowWithMetadata(b.Metadata(), calcSeries...)
}
