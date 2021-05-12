package bow

import (
	"fmt"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"sync"
)

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
				var newArray array.Interface
				prevData := b.Record.Column(colIndex).Data()
				pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
				valids := make([]bool, b.NumRows())
				switch typ {
				case Int64:
					prevArray := array.NewInt64Data(prevData)
					values := make([]int64, b.NumRows())
					for rowIndex := 1; rowIndex < b.NumRows(); rowIndex++ {
						values[rowIndex] = prevArray.Value(rowIndex) - prevArray.Value(rowIndex-1)
						valids[rowIndex] = prevArray.IsValid(rowIndex) && prevArray.IsValid(rowIndex-1)
					}
					build := array.NewInt64Builder(pool)
					build.AppendValues(values, valids)
					newArray = build.NewArray()
				case Float64:
					prevArray := array.NewFloat64Data(prevData)
					values := make([]float64, b.NumRows())
					for rowIndex := 1; rowIndex < b.NumRows(); rowIndex++ {
						values[rowIndex] = prevArray.Value(rowIndex) - prevArray.Value(rowIndex-1)
						valids[rowIndex] = prevArray.IsValid(rowIndex) && prevArray.IsValid(rowIndex-1)
					}
					build := array.NewFloat64Builder(pool)
					build.AppendValues(values, valids)
					newArray = build.NewArray()
				case Bool:
					prevArray := array.NewBooleanData(prevData)
					values := make([]bool, b.NumRows())
					for rowIndex := 1; rowIndex < b.NumRows(); rowIndex++ {
						values[rowIndex] = prevArray.Value(rowIndex) != prevArray.Value(rowIndex-1)
						valids[rowIndex] = prevArray.IsValid(rowIndex) && prevArray.IsValid(rowIndex-1)
					}
					build := array.NewBooleanBuilder(pool)
					build.AppendValues(values, valids)
					newArray = build.NewArray()
				}

				calcSeries[colIndex] = Series{
					Name:  colName,
					Array: newArray,
				}
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
