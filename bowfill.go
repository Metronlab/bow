package bow

import (
	"fmt"
	"math"
	"sync"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

// FillLinear fills the column toFillColName using the Linear interpolation method according
// to the reference column refColName, which has to be sorted.
// Fills only int64 and float64 types.
func (b *bow) FillLinear(refColName, toFillColName string) (Bow, error) {
	refIndex, err := b.GetColumnIndex(refColName)
	if err != nil {
		return nil, fmt.Errorf("bow: FillLinear: error with refColName: %w", err)
	}

	toFillIndex, err := b.GetColumnIndex(toFillColName)
	if err != nil {
		return nil, fmt.Errorf("bow: FillLinear: error with toFillColName: %w", err)
	}

	if refIndex == toFillIndex {
		return nil, fmt.Errorf("bow: FillLinear: refColName and toFillColName are equal")
	}

	switch b.GetType(refIndex) {
	case Int64:
	case Float64:
	default:
		return nil, fmt.Errorf("bow: FillLinear: refColName '%s' is of type '%s'",
			refColName, b.GetType(refIndex))
	}

	if b.IsColEmpty(refIndex) {
		return b, nil
	}

	if !b.IsColSorted(refIndex) {
		return nil, fmt.Errorf("bow: FillLinear: column '%s' is empty or not sorted", refColName)
	}

	switch b.GetType(toFillIndex) {
	case Int64:
	case Float64:
	default:
		return nil, fmt.Errorf(
			"bow: FillLinear: toFillColName '%s' is of type '%s'",
			toFillColName, b.GetType(toFillIndex))
	}

	var wg sync.WaitGroup
	filledSeries := make([]Series, b.NumCols())
	for colIndex, col := range b.Schema().Fields() {
		wg.Add(1)
		go func(colIndex int, colName string, wg *sync.WaitGroup) {
			defer wg.Done()
			if colIndex == toFillIndex {
				var newArray array.Interface
				prevData := b.Record.Column(colIndex).Data()
				pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
				switch b.GetType(colIndex) {
				case Int64:
					prevArray := array.NewInt64Data(prevData)
					values := prevArray.Int64Values()
					valids := getValids(prevArray, b.NumRows())
					for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
						if !valids[rowIndex] {
							prevToFill, rowPrev := b.GetPreviousFloat64(colIndex, rowIndex-1)
							nextToFill, rowNext := b.GetNextFloat64(colIndex, rowIndex+1)
							rowRef, valid1 := b.GetFloat64(refIndex, rowIndex)
							prevRef, valid2 := b.GetFloat64(refIndex, rowPrev)
							nextRef, valid3 := b.GetFloat64(refIndex, rowNext)
							if valid1 && valid2 && valid3 {
								if nextRef-prevRef != 0 {
									tmp := rowRef - prevRef
									tmp /= nextRef - prevRef
									tmp *= nextToFill - prevToFill
									tmp += prevToFill
									values[rowIndex] = int64(math.Round(tmp))
								} else {
									values[rowIndex] = int64(prevToFill)
								}
								valids[rowIndex] = true
							}
						}
					}
					build := array.NewInt64Builder(pool)
					build.AppendValues(values, valids)
					newArray = build.NewArray()
				case Float64:
					prevArray := array.NewFloat64Data(prevData)
					values := prevArray.Float64Values()
					valids := getValids(prevArray, b.NumRows())
					for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
						if !valids[rowIndex] {
							prevToFill, rowPrev := b.GetPreviousFloat64(colIndex, rowIndex-1)
							nextToFill, rowNext := b.GetNextFloat64(colIndex, rowIndex+1)
							rowRef, valid1 := b.GetFloat64(refIndex, rowIndex)
							prevRef, valid2 := b.GetFloat64(refIndex, rowPrev)
							nextRef, valid3 := b.GetFloat64(refIndex, rowNext)
							if valid1 && valid2 && valid3 {
								if nextRef-prevRef != 0.0 {
									values[rowIndex] = rowRef - prevRef
									values[rowIndex] /= nextRef - prevRef
									values[rowIndex] *= nextToFill - prevToFill
									values[rowIndex] += prevToFill
								} else {
									values[rowIndex] = prevToFill
								}
								valids[rowIndex] = true
							}
						}
					}
					build := array.NewFloat64Builder(pool)
					build.AppendValues(values, valids)
					newArray = build.NewArray()
				}
				filledSeries[colIndex] = Series{
					Name:  colName,
					Array: newArray,
				}
			} else {
				filledSeries[colIndex] = Series{
					Name:  colName,
					Array: b.Record.Column(colIndex),
				}
			}
		}(colIndex, col.Name, &wg)
	}
	wg.Wait()
	return NewBow(filledSeries...)
}

// FillMean fills nil values of `colNames` columns (`colNames` defaults to all columns)
// with the mean between the previous and the next values of the same column.
// Fills only int64 and float64 types.
func (b *bow) FillMean(colNames ...string) (Bow, error) {
	toFillCols, err := selectCols(b, colNames)
	if err != nil {
		return nil, fmt.Errorf(
			"bow: FillMean error selecting columns [%s] on bow schema [%s]: %w",
			colNames, b.Schema().String(), err)
	}

	for colIndex, col := range b.Schema().Fields() {
		if toFillCols[colIndex] {
			switch b.GetType(colIndex) {
			case Int64:
			case Float64:
			default:
				return nil, fmt.Errorf(
					"bow: FillMean type error: column '%s' is of type '%s'",
					col.Name, b.GetType(colIndex))
			}
		}
	}

	var wg sync.WaitGroup
	filledSeries := make([]Series, b.NumCols())
	for colIndex, col := range b.Schema().Fields() {
		wg.Add(1)
		go func(colIndex int, colName string, wg *sync.WaitGroup) {
			defer wg.Done()
			typ := b.GetType(colIndex)
			if toFillCols[colIndex] {
				var newArray array.Interface
				prevData := b.Record.Column(colIndex).Data()
				pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
				switch typ {
				case Int64:
					prevArray := array.NewInt64Data(prevData)
					values := prevArray.Int64Values()
					valids := getValids(prevArray, b.NumRows())
					for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
						if !valids[rowIndex] {
							prevVal, prevRow := b.GetPreviousFloat64(colIndex, rowIndex-1)
							nextVal, nextRow := b.GetNextFloat64(colIndex, rowIndex+1)
							if prevRow > -1 && nextRow > -1 {
								values[rowIndex] = int64(math.Round((prevVal + nextVal) / 2))
								valids[rowIndex] = true
							}
						}
					}
					build := array.NewInt64Builder(pool)
					build.AppendValues(values, valids)
					newArray = build.NewArray()
				case Float64:
					prevArray := array.NewFloat64Data(prevData)
					values := prevArray.Float64Values()
					valids := getValids(prevArray, b.NumRows())
					for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
						if !valids[rowIndex] {
							prevVal, prevRow := b.GetPreviousFloat64(colIndex, rowIndex-1)
							nextVal, nextRow := b.GetNextFloat64(colIndex, rowIndex+1)
							if prevRow > -1 && nextRow > -1 {
								values[rowIndex] = (prevVal + nextVal) / 2
								valids[rowIndex] = true
							}
						}
					}
					build := array.NewFloat64Builder(pool)
					build.AppendValues(values, valids)
					newArray = build.NewArray()
				}
				filledSeries[colIndex] = Series{
					Name:  colName,
					Array: newArray,
				}
			} else {
				filledSeries[colIndex] = Series{
					Name:  colName,
					Array: b.Record.Column(colIndex),
				}
			}
		}(colIndex, col.Name, &wg)
	}
	wg.Wait()
	return NewBow(filledSeries...)
}

// FillNext fills nil values of `colNames` columns (`colNames` defaults to all columns)
// using NOCB (Next Obs. Carried Backward) method.
func (b *bow) FillNext(colNames ...string) (Bow, error) {
	toFillCols, err := selectCols(b, colNames)
	if err != nil {
		return nil, fmt.Errorf(
			"bow: FillNext error selecting columns [%s] on bow schema [%s]: %w",
			colNames, b.Schema().String(), err)
	}

	var wg sync.WaitGroup
	filledSeries := make([]Series, b.NumCols())
	for colIndex, col := range b.Schema().Fields() {
		wg.Add(1)
		go func(colIndex int, colName string, wg *sync.WaitGroup) {
			defer wg.Done()
			typ := b.GetType(colIndex)
			if toFillCols[colIndex] {
				var newArray array.Interface
				prevData := b.Record.Column(colIndex).Data()
				pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
				switch typ {
				case Int64:
					prevArray := array.NewInt64Data(prevData)
					valids := getValids(prevArray, b.NumRows())
					values := make([]int64, b.NumRows())
					for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
						if !valids[rowIndex] {
							_, nextRow := b.GetNextValue(colIndex, rowIndex+1)
							if nextRow > -1 {
								values[rowIndex] = prevArray.Value(nextRow)
								valids[rowIndex] = true
							}
						} else {
							values[rowIndex] = prevArray.Value(rowIndex)
						}
					}
					build := array.NewInt64Builder(pool)
					build.AppendValues(values, valids)
					newArray = build.NewArray()
				case Float64:
					prevArray := array.NewFloat64Data(prevData)
					valids := getValids(prevArray, b.NumRows())
					values := make([]float64, b.NumRows())
					for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
						if !valids[rowIndex] {
							_, nextRow := b.GetNextValue(colIndex, rowIndex+1)
							if nextRow > -1 {
								values[rowIndex] = prevArray.Value(nextRow)
								valids[rowIndex] = true
							}
						} else {
							values[rowIndex] = prevArray.Value(rowIndex)
						}
					}
					build := array.NewFloat64Builder(pool)
					build.AppendValues(values, valids)
					newArray = build.NewArray()
				case Bool:
					prevArray := array.NewBooleanData(prevData)
					valids := getValids(prevArray, b.NumRows())
					values := make([]bool, b.NumRows())
					for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
						if !valids[rowIndex] {
							_, nextRow := b.GetNextValue(colIndex, rowIndex+1)
							if nextRow > -1 {
								values[rowIndex] = prevArray.Value(nextRow)
								valids[rowIndex] = true
							}
						} else {
							values[rowIndex] = prevArray.Value(rowIndex)
						}
					}
					build := array.NewBooleanBuilder(pool)
					build.AppendValues(values, valids)
					newArray = build.NewArray()
				case String:
					prevArray := array.NewStringData(prevData)
					valids := getValids(prevArray, b.NumRows())
					values := make([]string, b.NumRows())
					for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
						if !valids[rowIndex] {
							_, nextRow := b.GetNextValue(colIndex, rowIndex+1)
							if nextRow > -1 {
								values[rowIndex] = prevArray.Value(nextRow)
								valids[rowIndex] = true
							}
						} else {
							values[rowIndex] = prevArray.Value(rowIndex)
						}
					}
					build := array.NewStringBuilder(pool)
					build.AppendValues(values, valids)
					newArray = build.NewArray()
				default:
					newArray = b.Record.Column(colIndex)
				}
				filledSeries[colIndex] = Series{
					Name:  colName,
					Array: newArray,
				}
			} else {
				filledSeries[colIndex] = Series{
					Name:  colName,
					Array: b.Record.Column(colIndex),
				}
			}
		}(colIndex, col.Name, &wg)
	}
	wg.Wait()
	return NewBow(filledSeries...)
}

// FillPrevious fills nil values of `colNames` columns (`colNames` defaults to all columns)
// using LOCF (Last Obs. Carried Forward) method and returns a new Bow.
func (b *bow) FillPrevious(colNames ...string) (Bow, error) {
	toFillCols, err := selectCols(b, colNames)
	if err != nil {
		return nil, fmt.Errorf(
			"bow: FillPrevious error selecting columns [%s] on bow schema [%s]: %w",
			colNames, b.Schema().String(), err)
	}

	var wg sync.WaitGroup
	filledSeries := make([]Series, b.NumCols())
	for colIndex, col := range b.Schema().Fields() {
		wg.Add(1)
		go func(colIndex int, colName string, wg *sync.WaitGroup) {
			defer wg.Done()
			typ := b.GetType(colIndex)
			if toFillCols[colIndex] {
				var newArray array.Interface
				prevData := b.Record.Column(colIndex).Data()
				pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
				switch typ {
				case Int64:
					prevArray := array.NewInt64Data(prevData)
					valids := getValids(prevArray, b.NumRows())
					values := make([]int64, b.NumRows())
					for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
						if !valids[rowIndex] {
							_, prevRow := b.GetPreviousValue(colIndex, rowIndex-1)
							if prevRow > -1 {
								values[rowIndex] = prevArray.Value(prevRow)
								valids[rowIndex] = true
							}
						} else {
							values[rowIndex] = prevArray.Value(rowIndex)
						}
					}
					build := array.NewInt64Builder(pool)
					build.AppendValues(values, valids)
					newArray = build.NewArray()
				case Float64:
					prevArray := array.NewFloat64Data(prevData)
					valids := getValids(prevArray, b.NumRows())
					values := make([]float64, b.NumRows())
					for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
						if !valids[rowIndex] {
							_, prevRow := b.GetPreviousValue(colIndex, rowIndex-1)
							if prevRow > -1 {
								values[rowIndex] = prevArray.Value(prevRow)
								valids[rowIndex] = true
							}
						} else {
							values[rowIndex] = prevArray.Value(rowIndex)
						}
					}
					build := array.NewFloat64Builder(pool)
					build.AppendValues(values, valids)
					newArray = build.NewArray()
				case Bool:
					prevArray := array.NewBooleanData(prevData)
					valids := getValids(prevArray, b.NumRows())
					values := make([]bool, b.NumRows())
					for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
						if !valids[rowIndex] {
							_, prevRow := b.GetPreviousValue(colIndex, rowIndex-1)
							if prevRow > -1 {
								values[rowIndex] = prevArray.Value(prevRow)
								valids[rowIndex] = true
							}
						} else {
							values[rowIndex] = prevArray.Value(rowIndex)
						}
					}
					build := array.NewBooleanBuilder(pool)
					build.AppendValues(values, valids)
					newArray = build.NewArray()
				case String:
					prevArray := array.NewStringData(prevData)
					valids := getValids(prevArray, b.NumRows())
					values := make([]string, b.NumRows())
					for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
						if !valids[rowIndex] {
							_, prevRow := b.GetPreviousValue(colIndex, rowIndex-1)
							if prevRow > -1 {
								values[rowIndex] = prevArray.Value(prevRow)
								valids[rowIndex] = true
							}
						} else {
							values[rowIndex] = prevArray.Value(rowIndex)
						}
					}
					build := array.NewStringBuilder(pool)
					build.AppendValues(values, valids)
					newArray = build.NewArray()
				default:
					newArray = b.Record.Column(colIndex)
				}
				filledSeries[colIndex] = Series{
					Name:  colName,
					Array: newArray,
				}
			} else {
				filledSeries[colIndex] = Series{
					Name:  colName,
					Array: b.Record.Column(colIndex),
				}
			}
		}(colIndex, col.Name, &wg)
	}
	wg.Wait()
	return NewBow(filledSeries...)
}

// selectCols returns a bool slice of size b.NumCols
// with 'true' values at indexes of the corresponding colNames
func selectCols(b *bow, colNames []string) ([]bool, error) {
	toFill := make([]bool, b.NumCols())
	nilColsNb := len(colNames)
	// default: all columns to fill
	if nilColsNb == 0 {
		for colIndex := range b.Schema().Fields() {
			toFill[colIndex] = true
		}
	} else {
		for _, colName := range colNames {
			foundColIndex, err := b.GetColumnIndex(colName)
			if err != nil {
				return nil, err
			}
			toFill[foundColIndex] = true
		}
	}
	return toFill, nil
}

func getValids(arr array.Interface, length int) []bool {
	valids := make([]bool, length)

	for i := 0; i < length; i++ {
		valids[i] = arr.IsValid(i)
	}
	return valids
}
