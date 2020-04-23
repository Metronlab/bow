package bow

import (
	"fmt"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"math"
	"sync"
)

// FillLinear fills the column toFillCol using the Linear interpolation method according
// to the reference column refCol, which has to be sorted, and returns a new Bow.
// Fills values only for int64 and float64 numeric types.
func (b *bow) FillLinear(refCol string, toFillCol string) (Bow, error) {
	if refCol == toFillCol {
		err := fmt.Errorf("bow: FillLinear: reference and column to fill are equal")
		return nil, err
	}
	refIndex, err := b.GetIndex(refCol)
	if err != nil {
		return nil, err
	}

	switch b.GetType(refIndex) {
	case Int64:
	case Float64:
	default:
		err := fmt.Errorf("bow: FillLinear: reference column (refCol) '%s' is of type '%s'", refCol, b.GetType(refIndex))
		return nil, err
	}

	sorted := b.IsColSorted(refIndex)
	if !sorted {
		return nil, fmt.Errorf("bow: FillLinear: column '%s' is empty or not sorted", refCol)
	}
	toFillIndex, err := b.GetIndex(toFillCol)
	if err != nil {
		return nil, err
	}

	switch b.GetType(toFillIndex) {
	case Int64:
	case Float64:
	default:
		err := fmt.Errorf("bow: FillLinear: column '%s' is of type '%s'", toFillCol, b.GetType(toFillIndex))
		return nil, err
	}

	var wg sync.WaitGroup
	filledSeries := make([]Series, b.NumCols())
	length := b.NumRows()
	for colIndex, col := range b.Schema().Fields() {
		wg.Add(1)
		go func(colIndex int, colName string, wg *sync.WaitGroup) {
			defer wg.Done()
			typ := b.GetType(colIndex)
			if colIndex == toFillIndex {
				var newArray array.Interface
				prevData := b.Record.Column(colIndex).Data()
				pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
				switch typ {
				case Int64:
					prevArray := array.NewInt64Data(prevData)
					values := prevArray.Int64Values()
					valids := getValids(prevArray.NullBitmapBytes(), length)
					for rowIndex := 0; rowIndex < length; rowIndex++ {
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
					valids := getValids(prevArray.NullBitmapBytes(), length)
					for rowIndex := 0; rowIndex < length; rowIndex++ {
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
				filledSeries[b.GetColNameIndexUnsafe(colName)] = Series{
					Name:  colName,
					Array: newArray,
				}
			} else {
				filledSeries[b.GetColNameIndexUnsafe(colName)] = Series{
					Name:  colName,
					Array: b.Record.Column(colIndex),
				}
			}
		}(colIndex, col.Name, &wg)
	}
	wg.Wait()
	return NewBow(filledSeries...)
}

// FillMean fills any row that contains a nil for any of `nilCols`
// by the mean between the previous and the next values and returns a new Bow.
// Fills values only for int64 and float64 numeric types.
// (`colNames` defaults to all columns)
func (b *bow) FillMean(colNames ...string) (Bow, error) {
	toFill, err := colsToFill(b, colNames)
	if err != nil {
		return nil, err
	}

	for colIndex, col := range b.Schema().Fields() {
		if toFill[colIndex] {
			switch b.GetType(colIndex) {
			case Int64:
			case Float64:
			default:
				err = fmt.Errorf("fill mean type error: column '%s' is of type '%s'", col.Name, b.GetType(colIndex))
				return nil, err
			}
		}
	}

	var wg sync.WaitGroup
	filledSeries := make([]Series, b.NumCols())
	length := b.NumRows()
	for colIndex, col := range b.Schema().Fields() {
		wg.Add(1)
		go func(colIndex int, colName string, wg *sync.WaitGroup) {
			defer wg.Done()
			typ := b.GetType(colIndex)
			if toFill[colIndex] {
				var newArray array.Interface
				prevData := b.Record.Column(colIndex).Data()
				pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
				switch typ {
				case Int64:
					prevArray := array.NewInt64Data(prevData)
					values := prevArray.Int64Values()
					valids := getValids(prevArray.NullBitmapBytes(), length)
					for rowIndex := 0; rowIndex < length; rowIndex++ {
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
					valids := getValids(prevArray.NullBitmapBytes(), length)
					for rowIndex := 0; rowIndex < length; rowIndex++ {
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
				filledSeries[b.GetColNameIndexUnsafe(colName)] = Series{
					Name:  colName,
					Array: newArray,
				}
			} else {
				filledSeries[b.GetColNameIndexUnsafe(colName)] = Series{
					Name:  colName,
					Array: b.Record.Column(colIndex),
				}
			}
		}(colIndex, col.Name, &wg)
	}
	wg.Wait()
	return NewBow(filledSeries...)
}

// FillNext fills any row that contains a nil for any of `nilCols`
// using NOCB (Next Obs. Carried Backward) method and returns a new Bow.
// (`colNames` defaults to all columns)
func (b *bow) FillNext(colNames ...string) (Bow, error) {
	toFill, err := colsToFill(b, colNames)
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	filledSeries := make([]Series, b.NumCols())
	length := b.NumRows()
	for colIndex, col := range b.Schema().Fields() {
		wg.Add(1)
		go func(colIndex int, colName string, wg *sync.WaitGroup) {
			defer wg.Done()
			typ := b.GetType(colIndex)
			if toFill[colIndex] {
				var newArray array.Interface
				prevData := b.Record.Column(colIndex).Data()
				pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
				switch typ {
				case Int64:
					prevArray := array.NewInt64Data(prevData)
					valids := getValids(prevArray.NullBitmapBytes(), length)
					values := make([]int64, length)
					for rowIndex := 0; rowIndex < length; rowIndex++ {
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
					valids := getValids(prevArray.NullBitmapBytes(), length)
					values := make([]float64, length)
					for rowIndex := 0; rowIndex < length; rowIndex++ {
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
					valids := getValids(prevArray.NullBitmapBytes(), length)
					values := make([]bool, length)
					for rowIndex := 0; rowIndex < length; rowIndex++ {
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
					valids := getValids(prevArray.NullBitmapBytes(), length)
					values := make([]string, length)
					for rowIndex := 0; rowIndex < length; rowIndex++ {
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
				filledSeries[b.GetColNameIndexUnsafe(colName)] = Series{
					Name:  colName,
					Array: newArray,
				}
			} else {
				filledSeries[b.GetColNameIndexUnsafe(colName)] = Series{
					Name:  colName,
					Array: b.Record.Column(colIndex),
				}
			}
		}(colIndex, col.Name, &wg)
	}
	wg.Wait()
	return NewBow(filledSeries...)
}

// FillPrevious fills any row that contains a nil for any of `nilCols`
// using LOCF (Last Obs. Carried Forward) method and returns a new Bow.
// (`colNames` defaults to all columns)
func (b *bow) FillPrevious(colNames ...string) (Bow, error) {
	toFill, err := colsToFill(b, colNames)
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	filledSeries := make([]Series, b.NumCols())
	length := b.NumRows()
	for colIndex, col := range b.Schema().Fields() {
		wg.Add(1)
		go func(colIndex int, colName string, wg *sync.WaitGroup) {
			defer wg.Done()
			typ := b.GetType(colIndex)
			if toFill[colIndex] {
				var newArray array.Interface
				prevData := b.Record.Column(colIndex).Data()
				pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
				switch typ {
				case Int64:
					prevArray := array.NewInt64Data(prevData)
					valids := getValids(prevArray.NullBitmapBytes(), length)
					values := make([]int64, length)
					for rowIndex := 0; rowIndex < length; rowIndex++ {
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
					valids := getValids(prevArray.NullBitmapBytes(), length)
					values := make([]float64, length)
					for rowIndex := 0; rowIndex < length; rowIndex++ {
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
					valids := getValids(prevArray.NullBitmapBytes(), length)
					values := make([]bool, length)
					for rowIndex := 0; rowIndex < length; rowIndex++ {
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
					valids := getValids(prevArray.NullBitmapBytes(), length)
					values := make([]string, length)
					for rowIndex := 0; rowIndex < length; rowIndex++ {
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
				filledSeries[b.GetColNameIndexUnsafe(colName)] = Series{
					Name:  colName,
					Array: newArray,
				}
			} else {
				filledSeries[b.GetColNameIndexUnsafe(colName)] = Series{
					Name:  colName,
					Array: b.Record.Column(colIndex),
				}
			}
		}(colIndex, col.Name, &wg)
	}
	wg.Wait()
	return NewBow(filledSeries...)
}

// colsToFill returns a bool slice of size b.NumCols
// with 'true' values at indexes of the corresponding colNames
func colsToFill(b *bow, colNames []string) ([]bool, error) {
	toFill := make([]bool, b.NumCols())
	nilColsNb := len(colNames)
	// default: all columns to fill
	if nilColsNb == 0 {
		for colIndex := range b.Schema().Fields() {
			toFill[colIndex] = true
		}
	} else {
		for _, colName := range colNames {
			foundColIndex, err := b.GetIndex(colName)
			if err != nil {
				return nil, err
			}
			toFill[foundColIndex] = true
		}
	}
	return toFill, nil
}

var bitMask = [8]byte{1, 2, 4, 8, 16, 32, 64, 128}

// TODO: use function that is now exposed in arrow in a more recent release
// bitIsSet returns true if the bit at index i in buf is set (1).
func bitIsSet(buf []byte, i int) bool { return (buf[uint(i)/8] & bitMask[byte(i)%8]) != 0 }

func getValids(bytes []byte, size int) []bool {
	valids := make([]bool, size)

	for i := 0; i < size; i++ {
		valids[i] = bitIsSet(bytes, i)
	}
	return valids
}
