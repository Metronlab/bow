package bow

import (
	"fmt"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"math"
)

// FillLinear fills the column toFillCol using the Linear interpolation method according
// to the reference column refCol, which has to be sorted, and returns a new Bow.
func (b *bow) FillLinear(refCol string, toFillCol string) (Bow, error) {
	if refCol == toFillCol {
		err := fmt.Errorf("bow: FillLinear: reference and column to fill are equal")
		return nil, err
	}
	refIndex, err := b.GetIndex(refCol)
	if err != nil {
		return nil, err
	}
	refType := b.GetType(refIndex)
	if refType != Float64 && refType != Int64 {
		err := fmt.Errorf("bow: FillLinear: reference column (refCol) '%s' is of type '%s'", refCol, refType)
		return nil, err
	}
	sorted, err := b.IsColSorted(refIndex)
	if !sorted && err == nil {
		return nil, fmt.Errorf("bow: FillLinear: column '%s' is not sorted", refCol)
	} else if !sorted && err != nil {
		return nil, err
	}
	toFillIndex, err := b.GetIndex(toFillCol)
	if err != nil {
		return nil, err
	}
	toFillType := b.GetType(toFillIndex)
	if toFillType != Float64 && toFillType != Int64 {
		err := fmt.Errorf("bow: FillLinear: column '%s' is of type '%s'", toFillCol, toFillType)
		return nil, err
	}

	seriesChannel := make(chan Series, b.NumCols())
	for colIndex, col := range b.Schema().Fields() {
		go func(colIndex int, colName string) {
			typ := b.GetType(colIndex)
			var newArray array.Interface
			prevData := b.Record.Column(colIndex).Data()
			if colIndex == toFillIndex {
				switch typ {
				case Int64:
					prevArray := array.NewInt64Data(prevData)
					values := prevArray.Int64Values()
					valids := getValids(prevArray.NullBitmapBytes(), len(values))
					for rowIndex := 0; rowIndex < len(valids); rowIndex++ {
						if !valids[rowIndex] {
							refInt, valid1 := b.GetInt64(refIndex, rowIndex)
							prevToFillInt, rowPrev := b.GetPreviousInt64(colIndex, rowIndex-1)
							nextToFillInt, rowNext := b.GetNextInt64(colIndex, rowIndex+1)
							prevRefInt, valid2 := b.GetInt64(refIndex, rowPrev)
							nextRefInt, valid3 := b.GetInt64(refIndex, rowNext)
							if valid1 && valid2 && valid3 && nextRefInt-prevRefInt != 0 {
								tmp := float64(refInt) - float64(prevRefInt)
								tmp /= float64(nextRefInt) - float64(prevRefInt)
								tmp *= float64(nextToFillInt) - float64(prevToFillInt)
								tmp += float64(prevToFillInt)
								values[rowIndex] = int64(math.Round(tmp))
								valids[rowIndex] = true
							} else if valid1 && valid2 && valid3 && nextRefInt == prevRefInt {
								values[rowIndex] = prevToFillInt
								valids[rowIndex] = true
							}
						}
					}
					pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
					b := array.NewInt64Builder(pool)
					b.AppendValues(values, valids)
					newArray = b.NewArray()
				case Float64:
					prevArray := array.NewFloat64Data(prevData)
					values := prevArray.Float64Values()
					valids := getValids(prevArray.NullBitmapBytes(), len(values))
					for rowIndex := 0; rowIndex < len(valids); rowIndex++ {
						if !valids[rowIndex] {
							refFloat, valid1 := b.GetFloat64(refIndex, rowIndex)
							prevToFillFloat, rowPrev := b.GetPreviousFloat64(colIndex, rowIndex-1)
							nextToFillFloat, rowNext := b.GetNextFloat64(colIndex, rowIndex+1)
							prevRefFloat, valid2 := b.GetFloat64(refIndex, rowPrev)
							nextRefFloat, valid3 := b.GetFloat64(refIndex, rowNext)
							if valid1 && valid2 && valid3 && nextRefFloat-prevRefFloat != 0.0 {
								values[rowIndex] = refFloat - prevRefFloat
								values[rowIndex] /= nextRefFloat - prevRefFloat
								values[rowIndex] *= nextToFillFloat - prevToFillFloat
								values[rowIndex] += prevToFillFloat
								valids[rowIndex] = true
							} else if valid1 && valid2 && valid3 && nextRefFloat == prevRefFloat {
								values[rowIndex] = prevToFillFloat
								valids[rowIndex] = true
							}
						}
					}
					pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
					b := array.NewFloat64Builder(pool)
					b.AppendValues(values, valids)
					newArray = b.NewArray()
				}
			} else {
				newArray = array.MakeFromData(prevData)
			}
			seriesChannel <- Series{
				Name:  colName,
				Array: newArray,
			}
		}(colIndex, col.Name)
	}
	return newBowFromSeriesChannel(b, seriesChannel)
}

// FillMean fills any row that contains a nil for any of `nilCols`
// by the mean between the previous and the next values and returns a new Bow.
// (`colNames` defaults to all columns)
func (b *bow) FillMean(colNames ...string) (Bow, error) {
	toFill, err := colsToFill(b, colNames)
	if err != nil {
		return nil, err
	}

	seriesChannel := make(chan Series, b.NumCols())
	for colIndex, col := range b.Schema().Fields() {
		go func(colIndex int, colName string) {
			typ := b.GetType(colIndex)
			var newArray array.Interface
			prevData := b.Record.Column(colIndex).Data()
			if toFill[colIndex] {
				switch typ {
				case Int64:
					prevArray := array.NewInt64Data(prevData)
					values := prevArray.Int64Values()
					valids := getValids(prevArray.NullBitmapBytes(), len(values))
					for rowIndex := 0; rowIndex < len(valids); rowIndex++ {
						if !valids[rowIndex] {
							prevInt, prevRow := b.GetPreviousInt64(colIndex, rowIndex-1)
							nextInt, nextRow := b.GetNextInt64(colIndex, rowIndex+1)
							if prevRow > -1 && nextRow > -1 {
								tmp := (float64(prevInt) + float64(nextInt)) / 2
								values[rowIndex] = int64(math.Round(tmp))
								valids[rowIndex] = true
							}
						}
					}
					pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
					b := array.NewInt64Builder(pool)
					b.AppendValues(values, valids)
					newArray = b.NewArray()
				case Float64:
					prevArray := array.NewFloat64Data(prevData)
					values := prevArray.Float64Values()
					valids := getValids(prevArray.NullBitmapBytes(), len(values))
					for rowIndex := 0; rowIndex < len(valids); rowIndex++ {
						if !valids[rowIndex] {
							prevFloat, prevRow := b.GetPreviousFloat64(colIndex, rowIndex-1)
							nextFloat, nextRow := b.GetNextFloat64(colIndex, rowIndex+1)
							if prevRow > -1 && nextRow > -1 {
								values[rowIndex] = (prevFloat + nextFloat) / 2
								valids[rowIndex] = true
							}
						}
					}
					pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
					b := array.NewFloat64Builder(pool)
					b.AppendValues(values, valids)
					newArray = b.NewArray()
				}
			} else {
				newArray = array.MakeFromData(prevData)
			}
			seriesChannel <- Series{
				Name:  colName,
				Array: newArray,
			}
		}(colIndex, col.Name)
	}
	return newBowFromSeriesChannel(b, seriesChannel)
}

// FillNext fills any row that contains a nil for any of `nilCols`
// using NOCB (Next Obs. Carried Backward) method and returns a new Bow.
// (`colNames` defaults to all columns)
func (b *bow) FillNext(colNames ...string) (Bow, error) {
	toFill, err := colsToFill(b, colNames)
	if err != nil {
		return nil, err
	}

	seriesChannel := make(chan Series, b.NumCols())
	for colIndex, col := range b.Schema().Fields() {
		go func(colIndex int, colName string) {
			typ := b.GetType(colIndex)
			var newArray array.Interface
			prevData := b.Record.Column(colIndex).Data()
			if toFill[colIndex] {
				switch typ {
				case Int64:
					prevArray := array.NewInt64Data(prevData)
					values := prevArray.Int64Values()
					valids := getValids(prevArray.NullBitmapBytes(), len(values))
					for rowIndex := 0; rowIndex < len(valids); rowIndex++ {
						if !valids[rowIndex] {
							nextInt, nextRow := b.GetNextInt64(colIndex, rowIndex+1)
							if nextRow > -1 {
								values[rowIndex] = nextInt
								valids[rowIndex] = true
							}
						}
					}
					pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
					b := array.NewInt64Builder(pool)
					b.AppendValues(values, valids)
					newArray = b.NewArray()
				case Float64:
					prevArray := array.NewFloat64Data(prevData)
					values := prevArray.Float64Values()
					valids := getValids(prevArray.NullBitmapBytes(), len(values))
					for rowIndex := 0; rowIndex < len(valids); rowIndex++ {
						if !valids[rowIndex] {
							nextFloat, nextRow := b.GetNextFloat64(colIndex, rowIndex+1)
							if nextRow > -1 {
								values[rowIndex] = nextFloat
								valids[rowIndex] = true
							}
						}
					}
					pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
					b := array.NewFloat64Builder(pool)
					b.AppendValues(values, valids)
					newArray = b.NewArray()
				}
			} else {
				newArray = array.MakeFromData(prevData)
			}
			seriesChannel <- Series{
				Name:  colName,
				Array: newArray,
			}
		}(colIndex, col.Name)
	}
	return newBowFromSeriesChannel(b, seriesChannel)
}

// FillPrevious fills any row that contains a nil for any of `nilCols`
// using LOCF (Last Obs. Carried Forward) method and returns a new Bow.
// (`colNames` defaults to all columns)
func (b *bow) FillPrevious(colNames ...string) (Bow, error) {
	toFill, err := colsToFill(b, colNames)
	if err != nil {
		return nil, err
	}

	seriesChannel := make(chan Series, b.NumCols())
	for colIndex, col := range b.Schema().Fields() {
		go func(colIndex int, colName string) {
			typ := b.GetType(colIndex)
			var newArray array.Interface
			prevData := b.Record.Column(colIndex).Data()
			if toFill[colIndex] {
				switch typ {
				case Int64:
					prevArray := array.NewInt64Data(prevData)
					values := prevArray.Int64Values()
					valids := getValids(prevArray.NullBitmapBytes(), len(values))
					for rowIndex := 0; rowIndex < len(valids); rowIndex++ {
						if !valids[rowIndex] {
							prevInt, prevRow := b.GetPreviousInt64(colIndex, rowIndex-1)
							if prevRow > -1 {
								values[rowIndex] = prevInt
								valids[rowIndex] = true
							}
						}
					}
					pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
					b := array.NewInt64Builder(pool)
					b.AppendValues(values, valids)
					newArray = b.NewArray()
				case Float64:
					prevArray := array.NewFloat64Data(prevData)
					values := prevArray.Float64Values()
					valids := getValids(prevArray.NullBitmapBytes(), len(values))
					for rowIndex := 0; rowIndex < len(valids); rowIndex++ {
						if !valids[rowIndex] {
							prevFloat, prevRow := b.GetPreviousFloat64(colIndex, rowIndex-1)
							if prevRow > -1 {
								values[rowIndex] = prevFloat
								valids[rowIndex] = true
							}
						}
					}
					pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
					b := array.NewFloat64Builder(pool)
					b.AppendValues(values, valids)
					newArray = b.NewArray()
				}
			} else {
				newArray = array.MakeFromData(prevData)
			}
			seriesChannel <- Series{
				Name:  colName,
				Array: newArray,
			}
		}(colIndex, col.Name)
	}
	return newBowFromSeriesChannel(b, seriesChannel)
}

func newBowFromSeriesChannel(b *bow, seriesChannel chan Series) (Bow, error) {
	seriesCounter := 0
	filledSeries := make([]Series, b.NumCols())
	for s := range seriesChannel {
		for colIndex, col := range b.Schema().Fields() {
			if s.Name == col.Name {
				filledSeries[colIndex] = s
				seriesCounter++
				if seriesCounter == b.NumCols() {
					close(seriesChannel)
				}
			}
		}
	}
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

// bitIsSet returns true if the bit at index i in buf is set (1).
func bitIsSet(buf []byte, i int) bool { return (buf[uint(i)/8] & bitMask[byte(i)%8]) != 0 }

func getValids(bytes []byte, size int) []bool {
	valids := make([]bool, size)

	for i := 0; i < size; i++ {
		if bitIsSet(bytes, i) {
			valids[i] = true
		} else {
			valids[i] = false
		}
	}
	return valids
}
