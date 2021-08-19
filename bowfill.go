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
	refIndex, err := b.ColumnIndex(refColName)
	if err != nil {
		return nil, fmt.Errorf("bow.FillLinear: refColName: %w", err)
	}

	toFillIndex, err := b.ColumnIndex(toFillColName)
	if err != nil {
		return nil, fmt.Errorf("bow.FillLinear: toFillColName: %w", err)
	}

	if refIndex == toFillIndex {
		return nil, fmt.Errorf("bow.FillLinear: refColName and toFillColName are equal")
	}

	switch b.ColumnType(refIndex) {
	case Int64:
	case Float64:
	default:
		return nil, fmt.Errorf("bow.FillLinear: refColName '%s' is of type '%s'",
			refColName, b.ColumnType(refIndex))
	}

	if b.IsColEmpty(refIndex) {
		return b, nil
	}

	if !b.IsColSorted(refIndex) {
		return nil, fmt.Errorf("bow.FillLinear: column '%s' is empty or not sorted", refColName)
	}

	switch b.ColumnType(toFillIndex) {
	case Int64:
	case Float64:
	default:
		return nil, fmt.Errorf(
			"bow.FillLinear: toFillColName '%s' is of unsupported type '%s'",
			toFillColName, b.ColumnType(toFillIndex))
	}

	var wg sync.WaitGroup
	filledSeries := make([]Series, b.NumCols())
	for colIndex, col := range b.Schema().Fields() {
		if colIndex != toFillIndex {
			filledSeries[colIndex] = b.NewSeriesFromCol(colIndex)
			continue
		}

		wg.Add(1)
		go func(toFillIndex int, colName string) {
			defer wg.Done()
			var newArray array.Interface
			colData := b.Column(toFillIndex).Data()
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			switch b.ColumnType(toFillIndex) {
			case Int64:
				arr := array.NewInt64Data(colData)
				values := arr.Int64Values()
				valid := getValiditySlice(arr)
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if valid[rowIndex] {
						continue
					}
					prevToFill, rowPrev := b.GetPreviousFloat64(toFillIndex, rowIndex-1)
					nextToFill, rowNext := b.GetNextFloat64(toFillIndex, rowIndex+1)
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
						valid[rowIndex] = true
					}
				}
				build := array.NewInt64Builder(pool)
				build.AppendValues(values, valid)
				newArray = build.NewArray()
			case Float64:
				arr := array.NewFloat64Data(colData)
				values := arr.Float64Values()
				valid := getValiditySlice(arr)
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if valid[rowIndex] {
						continue
					}
					prevToFill, rowPrev := b.GetPreviousFloat64(toFillIndex, rowIndex-1)
					nextToFill, rowNext := b.GetNextFloat64(toFillIndex, rowIndex+1)
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
						valid[rowIndex] = true
					}
				}
				build := array.NewFloat64Builder(pool)
				build.AppendValues(values, valid)
				newArray = build.NewArray()
			}
			filledSeries[toFillIndex] = Series{
				Name:  colName,
				Array: newArray,
			}
		}(colIndex, col.Name)
	}
	wg.Wait()

	return NewBowWithMetadata(b.Metadata(), filledSeries...)
}

// FillMean fills nil values of `colNames` columns (`colNames` defaults to all columns)
// with the mean between the previous and the next values of the same column.
// Fills only int64 and float64 types.
func (b *bow) FillMean(colNames ...string) (Bow, error) {
	toFillCols, err := selectCols(b, colNames)
	if err != nil {
		return nil, fmt.Errorf("bow: FillMean: %w", err)
	}

	for colIndex, col := range b.Schema().Fields() {
		if toFillCols[colIndex] {
			switch b.ColumnType(colIndex) {
			case Int64:
			case Float64:
			default:
				return nil, fmt.Errorf(
					"bow.FillMean: column '%s' is of unsupported type '%s'",
					col.Name, b.ColumnType(colIndex))
			}
		}
	}

	var wg sync.WaitGroup
	filledSeries := make([]Series, b.NumCols())
	for colIndex, col := range b.Schema().Fields() {
		if !toFillCols[colIndex] {
			filledSeries[colIndex] = b.NewSeriesFromCol(colIndex)
			continue
		}

		wg.Add(1)
		go func(colIndex int, colName string) {
			defer wg.Done()
			typ := b.ColumnType(colIndex)
			var newArray array.Interface
			colData := b.Column(colIndex).Data()
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			switch typ {
			case Int64:
				arr := array.NewInt64Data(colData)
				values := arr.Int64Values()
				valid := getValiditySlice(arr)
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if valid[rowIndex] {
						continue
					}
					prevVal, prevRow := b.GetPreviousFloat64(colIndex, rowIndex-1)
					nextVal, nextRow := b.GetNextFloat64(colIndex, rowIndex+1)
					if prevRow > -1 && nextRow > -1 {
						values[rowIndex] = int64(math.Round((prevVal + nextVal) / 2))
						valid[rowIndex] = true
					}
				}
				build := array.NewInt64Builder(pool)
				build.AppendValues(values, valid)
				newArray = build.NewArray()
			case Float64:
				arr := array.NewFloat64Data(colData)
				values := arr.Float64Values()
				valid := getValiditySlice(arr)
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if valid[rowIndex] {
						continue
					}
					prevVal, prevRow := b.GetPreviousFloat64(colIndex, rowIndex-1)
					nextVal, nextRow := b.GetNextFloat64(colIndex, rowIndex+1)
					if prevRow > -1 && nextRow > -1 {
						values[rowIndex] = (prevVal + nextVal) / 2
						valid[rowIndex] = true
					}
				}
				build := array.NewFloat64Builder(pool)
				build.AppendValues(values, valid)
				newArray = build.NewArray()
			}
			filledSeries[colIndex] = Series{
				Name:  colName,
				Array: newArray,
			}
		}(colIndex, col.Name)
	}
	wg.Wait()

	return NewBowWithMetadata(b.Metadata(), filledSeries...)
}

// FillNext fills nil values of `colNames` columns (`colNames` defaults to all columns)
// using NOCB (Next Obs. Carried Backward) method.
func (b *bow) FillNext(colNames ...string) (Bow, error) {
	return fill("Next", b, colNames...)
}

// FillPrevious fills nil values of `colNames` columns (`colNames` defaults to all columns)
// using LOCF (Last Obs. Carried Forward) method.
func (b *bow) FillPrevious(colNames ...string) (Bow, error) {
	return fill("Previous", b, colNames...)
}

func fill(method string, b *bow, colNames ...string) (Bow, error) {
	toFillCols, err := selectCols(b, colNames)
	if err != nil {
		return nil, fmt.Errorf(
			"bow.Fill%s error selecting columns [%s] on bow schema [%s]: %w",
			method, colNames, b.Schema().String(), err)
	}

	var wg sync.WaitGroup
	filledSeries := make([]Series, b.NumCols())
	for colIndex, col := range b.Schema().Fields() {
		if !toFillCols[colIndex] {
			filledSeries[colIndex] = b.NewSeriesFromCol(colIndex)
			continue
		}

		wg.Add(1)
		go func(colIndex int, colName string) {
			defer wg.Done()
			typ := b.ColumnType(colIndex)
			var newArray array.Interface
			colData := b.Column(colIndex).Data()
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			switch typ {
			case Int64:
				arr := array.NewInt64Data(colData)
				valid := getValiditySlice(arr)
				values := make([]int64, b.NumRows())
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if !valid[rowIndex] {
						var fillRowIndex int
						switch method {
						case "Previous":
							_, fillRowIndex = b.GetPreviousValue(colIndex, rowIndex-1)
						case "Next":
							_, fillRowIndex = b.GetNextValue(colIndex, rowIndex+1)
						default:
							panic(fmt.Errorf("bow.fill: method '%s' not supported", method))
						}
						if fillRowIndex > -1 {
							values[rowIndex] = arr.Value(fillRowIndex)
							valid[rowIndex] = true
						}
					} else {
						values[rowIndex] = arr.Value(rowIndex)
					}
				}
				build := array.NewInt64Builder(mem)
				build.AppendValues(values, valid)
				newArray = build.NewArray()
			case Float64:
				arr := array.NewFloat64Data(colData)
				valid := getValiditySlice(arr)
				values := make([]float64, b.NumRows())
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if !valid[rowIndex] {
						var fillRowIndex int
						switch method {
						case "Previous":
							_, fillRowIndex = b.GetPreviousValue(colIndex, rowIndex-1)
						case "Next":
							_, fillRowIndex = b.GetNextValue(colIndex, rowIndex+1)
						default:
							panic(fmt.Errorf("bow.fill: method '%s' not supported", method))
						}
						if fillRowIndex > -1 {
							values[rowIndex] = arr.Value(fillRowIndex)
							valid[rowIndex] = true
						}
					} else {
						values[rowIndex] = arr.Value(rowIndex)
					}
				}
				build := array.NewFloat64Builder(mem)
				build.AppendValues(values, valid)
				newArray = build.NewArray()
			case Bool:
				arr := array.NewBooleanData(colData)
				valid := getValiditySlice(arr)
				values := make([]bool, b.NumRows())
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if !valid[rowIndex] {
						var fillRowIndex int
						switch method {
						case "Previous":
							_, fillRowIndex = b.GetPreviousValue(colIndex, rowIndex-1)
						case "Next":
							_, fillRowIndex = b.GetNextValue(colIndex, rowIndex+1)
						default:
							panic(fmt.Errorf("bow.fill: method '%s' not supported", method))
						}
						if fillRowIndex > -1 {
							values[rowIndex] = arr.Value(fillRowIndex)
							valid[rowIndex] = true
						}
					} else {
						values[rowIndex] = arr.Value(rowIndex)
					}
				}
				build := array.NewBooleanBuilder(mem)
				build.AppendValues(values, valid)
				newArray = build.NewArray()
			case String:
				arr := array.NewStringData(colData)
				valid := getValiditySlice(arr)
				values := make([]string, b.NumRows())
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if !valid[rowIndex] {
						var fillRowIndex int
						switch method {
						case "Previous":
							_, fillRowIndex = b.GetPreviousValue(colIndex, rowIndex-1)
						case "Next":
							_, fillRowIndex = b.GetNextValue(colIndex, rowIndex+1)
						default:
							panic(fmt.Errorf("bow.fill: method '%s' not supported", method))
						}
						if fillRowIndex > -1 {
							values[rowIndex] = arr.Value(fillRowIndex)
							valid[rowIndex] = true
						}
					} else {
						values[rowIndex] = arr.Value(rowIndex)
					}
				}
				build := array.NewStringBuilder(mem)
				build.AppendValues(values, valid)
				newArray = build.NewArray()
			default:
				newArray = b.Column(colIndex)
			}
			filledSeries[colIndex] = Series{
				Name:  colName,
				Array: newArray,
			}
		}(colIndex, col.Name)
	}
	wg.Wait()

	return NewBowWithMetadata(b.Metadata(), filledSeries...)
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
			foundColIndex, err := b.ColumnIndex(colName)
			if err != nil {
				return nil, err
			}
			toFill[foundColIndex] = true
		}
	}
	return toFill, nil
}
