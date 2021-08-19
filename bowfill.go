package bow

import (
	"fmt"
	"math"
	"sync"

	"github.com/apache/arrow/go/arrow/array"
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
		if colIndex != toFillIndex || b.Column(colIndex).NullN() == 0 {
			filledSeries[colIndex] = b.NewSeriesFromCol(colIndex)
			continue
		}

		wg.Add(1)
		go func(toFillIndex int, colName string) {
			defer wg.Done()
			colData := b.Column(toFillIndex).Data()
			colBuf := b.NewBufferFromCol(toFillIndex)
			switch b.ColumnType(toFillIndex) {
			case Int64:
				arr := array.NewInt64Data(colData)
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if arr.IsValid(rowIndex) {
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
							colBuf.SetOrDropStrict(rowIndex, int64(math.Round(tmp)))
						} else {
							colBuf.SetOrDropStrict(rowIndex, int64(prevToFill))
						}
					}
				}
			case Float64:
				arr := array.NewFloat64Data(colData)
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if arr.IsValid(rowIndex) {
						continue
					}
					prevToFill, rowPrev := b.GetPreviousFloat64(toFillIndex, rowIndex-1)
					nextToFill, rowNext := b.GetNextFloat64(toFillIndex, rowIndex+1)
					rowRef, valid1 := b.GetFloat64(refIndex, rowIndex)
					prevRef, valid2 := b.GetFloat64(refIndex, rowPrev)
					nextRef, valid3 := b.GetFloat64(refIndex, rowNext)
					if valid1 && valid2 && valid3 {
						if nextRef-prevRef != 0.0 {
							tmp := rowRef - prevRef
							tmp /= nextRef - prevRef
							tmp *= nextToFill - prevToFill
							tmp += prevToFill
							colBuf.SetOrDropStrict(rowIndex, tmp)
						} else {
							colBuf.SetOrDropStrict(rowIndex, prevToFill)
						}
					}
				}
			}

			filledSeries[toFillIndex] = NewSeries(colName, b.ColumnType(toFillIndex), colBuf.Data, colBuf.nullBitmapBytes)

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
		return nil, fmt.Errorf("bow.FillMean: %w", err)
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
		if !toFillCols[colIndex] || b.Column(colIndex).NullN() == 0 {
			filledSeries[colIndex] = b.NewSeriesFromCol(colIndex)
			continue
		}

		wg.Add(1)
		go func(colIndex int, colName string) {
			defer wg.Done()
			buf := b.NewBufferFromCol(colIndex)
			switch b.ColumnType(colIndex) {
			case Int64:
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if buf.IsValid(rowIndex) {
						continue
					}
					prevVal, prevRow := b.GetPreviousFloat64(colIndex, rowIndex-1)
					nextVal, nextRow := b.GetNextFloat64(colIndex, rowIndex+1)
					if prevRow > -1 && nextRow > -1 {
						buf.SetOrDropStrict(rowIndex, int64(math.Round((prevVal+nextVal)/2)))
					}
				}
			case Float64:
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if buf.IsValid(rowIndex) {
						continue
					}
					prevVal, prevRow := b.GetPreviousFloat64(colIndex, rowIndex-1)
					nextVal, nextRow := b.GetNextFloat64(colIndex, rowIndex+1)
					if prevRow > -1 && nextRow > -1 {
						buf.SetOrDropStrict(rowIndex, (prevVal+nextVal)/2)
					}
				}
			}
			filledSeries[colIndex] = NewSeries(colName, b.ColumnType(colIndex), buf.Data, buf.nullBitmapBytes)

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
		return nil, fmt.Errorf("bow.Fill%s: %w", method, err)
	}

	var wg sync.WaitGroup
	filledSeries := make([]Series, b.NumCols())
	for colIndex, col := range b.Schema().Fields() {
		if !toFillCols[colIndex] || b.Column(colIndex).NullN() == 0 {
			filledSeries[colIndex] = b.NewSeriesFromCol(colIndex)
			continue
		}

		wg.Add(1)
		go func(colIndex int, colName string) {
			defer wg.Done()
			prevData := b.Column(colIndex).Data()
			buf := b.NewBufferFromCol(colIndex)
			switch b.ColumnType(colIndex) {
			case Int64:
				arr := array.NewInt64Data(prevData)
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if arr.IsValid(rowIndex) {
						continue
					}
					fillRowIndex := getFillRowIndex(b, method, colIndex, rowIndex)
					if fillRowIndex > -1 {
						buf.SetOrDropStrict(rowIndex, arr.Value(fillRowIndex))
					}
				}
			case Float64:
				arr := array.NewFloat64Data(prevData)
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if arr.IsValid(rowIndex) {
						continue
					}
					fillRowIndex := getFillRowIndex(b, method, colIndex, rowIndex)
					if fillRowIndex > -1 {
						buf.SetOrDropStrict(rowIndex, arr.Value(fillRowIndex))
					}
				}
			case Boolean:
				arr := array.NewBooleanData(prevData)
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if arr.IsValid(rowIndex) {
						continue
					}
					fillRowIndex := getFillRowIndex(b, method, colIndex, rowIndex)
					if fillRowIndex > -1 {
						buf.SetOrDropStrict(rowIndex, arr.Value(fillRowIndex))
					}
				}
			case String:
				arr := array.NewStringData(prevData)
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if arr.IsValid(rowIndex) {
						continue
					}
					fillRowIndex := getFillRowIndex(b, method, colIndex, rowIndex)
					if fillRowIndex > -1 {
						buf.SetOrDropStrict(rowIndex, arr.Value(fillRowIndex))
					}
				}
			default:
				filledSeries[colIndex] = b.NewSeriesFromCol(colIndex)
			}
			filledSeries[colIndex] = NewSeries(colName, b.ColumnType(colIndex), buf.Data, buf.nullBitmapBytes)
		}(colIndex, col.Name)
	}
	wg.Wait()

	return NewBowWithMetadata(b.Metadata(), filledSeries...)
}

func getFillRowIndex(b Bow, method string, colIndex, rowIndex int) int {
	switch method {
	case "Previous":
		return b.GetPreviousRowIndex(colIndex, rowIndex-1)
	case "Next":
		return b.GetNextRowIndex(colIndex, rowIndex+1)
	default:
		panic(fmt.Errorf("bow.Fill: method '%s' is not supported", method))
	}
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
