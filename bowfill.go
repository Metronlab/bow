package bow

import (
	"fmt"
	"math"
	"sync"
)

// FillLinear fills the column toFillColName using the Linear interpolation method according
// to the reference column refColName, which has to be sorted.
// Fills only int64 and float64 types.
func (b *bow) FillLinear(refColName, toFillColName string) (Bow, error) {
	refIndex, err := b.GetColIndex(refColName)
	if err != nil {
		return nil, fmt.Errorf("bow.FillLinear: %w", err)
	}

	toFillIndex, err := b.GetColIndex(toFillColName)
	if err != nil {
		return nil, fmt.Errorf("bow.FillLinear: %w", err)
	}

	if refIndex == toFillIndex {
		return nil, fmt.Errorf(
			"bow.FillLinear: refColName and toFillColName cannot have the same index")
	}

	switch b.GetColType(refIndex) {
	case Int64:
	case Float64:
	default:
		return nil, fmt.Errorf(
			"bow.FillLinear: refColName %q have unsupported type %q",
			refColName, b.GetColType(refIndex))
	}

	if b.IsColEmpty(refIndex) {
		return b, nil
	}

	if !b.IsColSorted(refIndex) {
		return nil, fmt.Errorf("bow.FillLinear: column %q is empty or not sorted", refColName)
	}

	toFillColType := b.GetColType(toFillIndex)
	switch toFillColType {
	case Int64:
	case Float64:
	default:
		return nil, fmt.Errorf(
			"bow.FillLinear: toFillColName %q is of unsupported type %q",
			toFillColName, toFillColType)
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
			toFillBuf := b.NewBufferFromCol(toFillIndex)
			switch toFillColType {
			case Int64:
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if b.Column(toFillIndex).IsValid(rowIndex) {
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
							toFillBuf.SetOrDrop(rowIndex, int64(math.Round(tmp)))
						} else {
							toFillBuf.SetOrDrop(rowIndex, prevToFill)
						}
					}
				}
			case Float64:
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if b.Column(toFillIndex).IsValid(rowIndex) {
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
							toFillBuf.SetOrDrop(rowIndex, tmp)
						} else {
							toFillBuf.SetOrDrop(rowIndex, prevToFill)
						}
					}
				}
			}
			filledSeries[toFillIndex] = NewSeries(colName, toFillColType, toFillBuf.Value, toFillBuf.Valid)
		}(toFillIndex, col.Name)
	}
	wg.Wait()

	return NewBowWithMetadata(
		Metadata{b.Schema().Metadata()},
		filledSeries...)
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
		if !toFillCols[colIndex] {
			continue
		}
		switch b.GetColType(colIndex) {
		case Int64:
		case Float64:
		default:
			return nil, fmt.Errorf(
				"bow.FillMean: column '%s' is of unsupported type '%s'",
				col.Name, b.GetColType(colIndex))
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
			toFillBuf := b.NewBufferFromCol(colIndex)
			colType := b.GetColType(colIndex)
			switch colType {
			case Int64:
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if toFillBuf.Valid[rowIndex] {
						continue
					}
					prevVal, prevRow := b.GetPreviousFloat64(colIndex, rowIndex-1)
					nextVal, nextRow := b.GetNextFloat64(colIndex, rowIndex+1)
					if prevRow > -1 && nextRow > -1 {
						toFillBuf.SetOrDrop(rowIndex, int64(math.Round((prevVal+nextVal)/2)))
					}
				}
			case Float64:
				for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
					if toFillBuf.Valid[rowIndex] {
						continue
					}
					prevVal, prevRow := b.GetPreviousFloat64(colIndex, rowIndex-1)
					nextVal, nextRow := b.GetNextFloat64(colIndex, rowIndex+1)
					if prevRow > -1 && nextRow > -1 {
						toFillBuf.SetOrDrop(rowIndex, (prevVal+nextVal)/2)
					}
				}
			}
			filledSeries[colIndex] = NewSeries(colName, colType, toFillBuf.Value, toFillBuf.Valid)
		}(colIndex, col.Name)
	}
	wg.Wait()
	return NewBowWithMetadata(
		Metadata{b.Schema().Metadata()},
		filledSeries...)
}

// FillNext fills nil values of `colNames` columns (`colNames` defaults to all columns)
// using NOCB (Next Obs. Carried Backward) method.
func (b *bow) FillNext(colNames ...string) (Bow, error) {
	return fill("Next", b, colNames...)
}

// FillPrevious fills nil values of `colNames` columns (`colNames` defaults to all columns)
// using LOCF (Last Obs. Carried Forward) method and returns a new Bow.
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
		if !toFillCols[colIndex] {
			filledSeries[colIndex] = b.NewSeriesFromCol(colIndex)
			continue
		}

		wg.Add(1)
		go func(colIndex int, colName string) {
			defer wg.Done()
			toFillBuf := b.NewBufferFromCol(colIndex)
			colType := b.GetColType(colIndex)
			for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
				if toFillBuf.Valid[rowIndex] {
					continue
				}
				var fillRowIndex int
				switch method {
				case "Previous":
					_, fillRowIndex = toFillBuf.GetPreviousValue(rowIndex - 1)
				case "Next":
					_, fillRowIndex = toFillBuf.GetNextValue(rowIndex + 1)
				default:
					panic(fmt.Errorf("bow.fill: method '%s' not supported", method))
				}
				if fillRowIndex > -1 {
					switch colType {
					case Int64:
						toFillBuf.SetOrDrop(rowIndex, toFillBuf.Value.([]int64)[fillRowIndex])
					case Float64:
						toFillBuf.SetOrDrop(rowIndex, toFillBuf.Value.([]float64)[fillRowIndex])
					case Bool:
						toFillBuf.SetOrDrop(rowIndex, toFillBuf.Value.([]bool)[fillRowIndex])
					case String:
						toFillBuf.SetOrDrop(rowIndex, toFillBuf.Value.([]string)[fillRowIndex])
					}
				}
			}
			filledSeries[colIndex] = NewSeries(colName, colType, toFillBuf.Value, toFillBuf.Valid)
		}(colIndex, col.Name)
	}
	wg.Wait()

	return NewBowWithMetadata(
		Metadata{b.Schema().Metadata()},
		filledSeries...)
}

// selectCols returns a bool slice of size b.NumCols
// with 'true' values at indexes of the corresponding colNames
func selectCols(b *bow, colNames []string) ([]bool, error) {
	toFillCols := make([]bool, b.NumCols())
	nilColsNb := len(colNames)

	// default: all columns to fill
	if nilColsNb == 0 {
		for colIndex := range b.Schema().Fields() {
			toFillCols[colIndex] = true
		}
	} else {
		for _, colName := range colNames {
			colIndex, err := b.GetColIndex(colName)
			if err != nil {
				return nil, fmt.Errorf("bow.selectCols: %w", err)
			}
			toFillCols[colIndex] = true
		}
	}

	return toFillCols, nil
}
