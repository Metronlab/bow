package bow

import (
	"fmt"
	"math"
	"sync"

	"github.com/apache/arrow/go/v8/arrow/array"
)

// FillLinear fills the column toFillColIndex using the Linear interpolation method according
// to the reference column refColIndex, which has to be sorted.
// Fills only int64 and float64 types.
func (b *bow) FillLinear(refColIndex, toFillColIndex int) (Bow, error) {
	if refColIndex < 0 || refColIndex > b.NumCols()-1 {
		return nil, fmt.Errorf("bow.FillLinear: refColIndex is out of range")
	}

	if toFillColIndex < 0 || toFillColIndex > b.NumCols()-1 {
		return nil, fmt.Errorf("bow.FillLinear: toFillColIndex is out of range")
	}

	if refColIndex == toFillColIndex {
		return nil, fmt.Errorf("bow.FillLinear: refColIndex and toFillColIndex are equal")
	}

	switch b.ColumnType(refColIndex) {
	case Int64:
	case Float64:
	default:
		return nil, fmt.Errorf("bow.FillLinear: refColIndex '%d' is of type '%s'",
			refColIndex, b.ColumnType(refColIndex))
	}

	if b.IsColEmpty(refColIndex) {
		return b, nil
	}

	if !b.IsColSorted(refColIndex) {
		return nil, fmt.Errorf("bow.FillLinear: refColIndex '%d' is empty or not sorted",
			refColIndex)
	}

	switch b.ColumnType(toFillColIndex) {
	case Int64:
	case Float64:
	default:
		return nil, fmt.Errorf(
			"bow.FillLinear: toFillColIndex '%d' is of unsupported type '%s'",
			toFillColIndex, b.ColumnType(toFillColIndex))
	}

	if b.Column(toFillColIndex).NullN() == 0 {
		return b, nil
	}
	buf := b.NewBufferFromCol(toFillColIndex)

	filledSeries := make([]Series, b.NumCols())
	for colIndex, col := range b.Schema().Fields() {
		if colIndex != toFillColIndex {
			filledSeries[colIndex] = b.NewSeriesFromCol(colIndex)
			continue
		}

		for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
			if buf.IsValid(rowIndex) {
				continue
			}
			prevToFill, rowPrev := b.GetPrevFloat64(toFillColIndex, rowIndex-1)
			nextToFill, rowNext := b.GetNextFloat64(toFillColIndex, rowIndex+1)
			rowRef, valid1 := b.GetFloat64(refColIndex, rowIndex)
			prevRef, valid2 := b.GetFloat64(refColIndex, rowPrev)
			nextRef, valid3 := b.GetFloat64(refColIndex, rowNext)
			if !valid1 || !valid2 || !valid3 {
				continue
			}

			if nextRef-prevRef == 0 {
				switch b.ColumnType(toFillColIndex) {
				case Int64:
					buf.SetOrDropStrict(rowIndex, int64(prevToFill))
				case Float64:
					buf.SetOrDropStrict(rowIndex, prevToFill)
				}
			}

			tmp := rowRef - prevRef
			tmp /= nextRef - prevRef
			tmp *= nextToFill - prevToFill
			tmp += prevToFill
			switch b.ColumnType(toFillColIndex) {
			case Int64:
				buf.SetOrDropStrict(rowIndex, int64(math.Round(tmp)))
			case Float64:
				buf.SetOrDropStrict(rowIndex, tmp)
			}
		}

		filledSeries[toFillColIndex] = NewSeriesFromBuffer(col.Name, buf)
	}

	return NewBowWithMetadata(b.Metadata(), filledSeries...)
}

// FillMean fills nil values of `colIndices` columns (`colIndices` defaults to all columns)
// with the mean between the previous and the next values of the same column.
// Fills only int64 and float64 types.
func (b *bow) FillMean(colIndices ...int) (Bow, error) {
	toFillCols, err := selectCols(b, colIndices)
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
			for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
				if buf.IsValid(rowIndex) {
					continue
				}
				prevVal, prevRow := b.GetPrevFloat64(colIndex, rowIndex-1)
				nextVal, nextRow := b.GetNextFloat64(colIndex, rowIndex+1)
				if prevRow > -1 && nextRow > -1 {
					switch b.ColumnType(colIndex) {
					case Int64:
						buf.SetOrDropStrict(rowIndex, int64(math.Round((prevVal+nextVal)/2)))
					case Float64:
						buf.SetOrDropStrict(rowIndex, (prevVal+nextVal)/2)
					}
				}
			}

			filledSeries[colIndex] = NewSeriesFromBuffer(colName, buf)

		}(colIndex, col.Name)
	}
	wg.Wait()

	return NewBowWithMetadata(b.Metadata(), filledSeries...)
}

// FillNext fills nil values of `colIndices` columns (`colIndices` defaults to all columns)
// using NOCB (Next Obs. Carried Backward) method.
func (b *bow) FillNext(colIndices ...int) (Bow, error) {
	return fill("Next", b, colIndices...)
}

// FillPrevious fills nil values of `colIndices` columns (`colIndices` defaults to all columns)
// using LOCF (Last Obs. Carried Forward) method.
func (b *bow) FillPrevious(colIndices ...int) (Bow, error) {
	return fill("Previous", b, colIndices...)
}

func fill(method string, b *bow, colIndices ...int) (Bow, error) {
	toFillCols, err := selectCols(b, colIndices)
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

			data := b.Column(colIndex).Data()
			buf := b.NewBufferFromCol(colIndex)
			switch b.ColumnType(colIndex) {
			case Int64:
				arr := array.NewInt64Data(data)
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if buf.IsValid(rowIndex) {
						continue
					}
					fillRowIndex := getFillRowIndex(b, method, colIndex, rowIndex)
					if fillRowIndex > -1 {
						buf.SetOrDropStrict(rowIndex, arr.Value(fillRowIndex))
					}
				}
			case Float64:
				arr := array.NewFloat64Data(data)
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if buf.IsValid(rowIndex) {
						continue
					}
					fillRowIndex := getFillRowIndex(b, method, colIndex, rowIndex)
					if fillRowIndex > -1 {
						buf.SetOrDropStrict(rowIndex, arr.Value(fillRowIndex))
					}
				}
			case Boolean:
				arr := array.NewBooleanData(data)
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if buf.IsValid(rowIndex) {
						continue
					}
					fillRowIndex := getFillRowIndex(b, method, colIndex, rowIndex)
					if fillRowIndex > -1 {
						buf.SetOrDropStrict(rowIndex, arr.Value(fillRowIndex))
					}
				}
			case String:
				arr := array.NewStringData(data)
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if buf.IsValid(rowIndex) {
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

			filledSeries[colIndex] = NewSeriesFromBuffer(colName, buf)

		}(colIndex, col.Name)
	}
	wg.Wait()

	return NewBowWithMetadata(b.Metadata(), filledSeries...)
}

func getFillRowIndex(b Bow, method string, colIndex, rowIndex int) int {
	switch method {
	case "Previous":
		return b.GetPrevRowIndex(colIndex, rowIndex-1)
	case "Next":
		return b.GetNextRowIndex(colIndex, rowIndex+1)
	default:
		panic(fmt.Errorf("bow.Fill: method '%s' is not supported", method))
	}
}

// selectCols returns a bool slice of size b.NumCols
// with 'true' values at indexes of the corresponding colIndices
func selectCols(b *bow, colIndices []int) ([]bool, error) {
	selectedCols := make([]bool, b.NumCols())

	// default: all columns selected
	if len(colIndices) == 0 {
		for colIndex := range b.Schema().Fields() {
			selectedCols[colIndex] = true
		}

		return selectedCols, nil
	}

	for _, colIndex := range colIndices {
		if colIndex < 0 || colIndex > b.NumCols()-1 {
			return nil, fmt.Errorf("selectCols: out of range colIndex '%d'", colIndex)
		}
		selectedCols[colIndex] = true
	}

	return selectedCols, nil
}
