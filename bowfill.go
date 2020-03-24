package bow

import (
	"fmt"

	"github.com/apache/arrow/go/arrow/array"
)

// FillLinear fills any row that contains a nil for any of `nilCols`
// in the column toFillCol using the Linear interpolation method according
// to the refCol and returns a new Bow.
// (`colNames` defaults to all columns)
func (b *bow) FillLinear(refCol string, toFillCol string) (Bow, error) {
	if refCol == toFillCol {
		err := fmt.Errorf("linear interpolation error: reference and column to fill are equal")
		return nil, err
	}
	refIndex, err := b.GetIndex(refCol)
	if err != nil {
		return nil, err
	}
	err = isColSorted(b, refIndex, b.GetType(refIndex))
	if err != nil {
		return nil, err
	}
	toFillIndex, err := b.GetIndex(toFillCol)
	if err != nil {
		return nil, err
	}
	refType := b.GetType(refIndex)
	toFillType := b.GetType(toFillIndex)
	if refType != Float64 && refType != Int64 {
		err := fmt.Errorf("linear fill type error: column '%s' is of type '%s'", refCol, refType)
		return nil, err
	}
	if toFillType != Float64 && toFillType != Int64 {
		err := fmt.Errorf("linear fill type error: column '%s' is of type '%s'", toFillCol, toFillType)
		return nil, err
	}

	seriesChannel := make(chan Series, b.NumCols())
	for colIndex, col := range b.Schema().Fields() {
		go func(colIndex int, colName string) {
			typ := b.GetType(colIndex)
			switch typ {
			case Int64:
				newArray := array.NewInt64Data(b.Record.Column(colIndex).Data())
				newBufValue := newArray.Int64Values()
				newBufValid := getInt64BufValid(newArray)
				if colIndex == toFillIndex {
					for rowIndex := 0; rowIndex < newArray.Len(); rowIndex++ {
						if !newBufValid[rowIndex] {
							refInt, valid1 := b.GetInt64(refIndex, rowIndex)
							prevToFillInt, rowPrev := b.GetPreviousInt64(colIndex, rowIndex-1)
							nextToFillInt, rowNext := b.GetNextInt64(colIndex, rowIndex+1)
							prevRefInt, valid2 := b.GetInt64(refIndex, rowPrev)
							nextRefInt, valid3 := b.GetInt64(refIndex, rowNext)
							if valid1 && valid2 && valid3 && nextRefInt-prevRefInt != 0 {
								newBufValue[rowIndex] = (refInt - prevRefInt) / (nextRefInt - prevRefInt)
								newBufValue[rowIndex] *= (nextToFillInt - prevToFillInt)
								newBufValue[rowIndex] += prevToFillInt
								newBufValid[rowIndex] = true
							}
						}
					}
				}
				seriesChannel <- Series{
					Name: colName,
					Type: typ,
					Data: Buffer{Value: newBufValue, Valid: newBufValid},
				}
			case Float64:
				newArray := array.NewFloat64Data(b.Record.Column(colIndex).Data())
				newBufValue := newArray.Float64Values()
				newBufValid := getFloat64BufValid(newArray)
				if colIndex == toFillIndex {
					for rowIndex := 0; rowIndex < newArray.Len(); rowIndex++ {
						if !newBufValid[rowIndex] {
							refFloat, valid1 := b.GetFloat64(refIndex, rowIndex)
							prevToFillFloat, rowPrev := b.GetPreviousFloat64(colIndex, rowIndex-1)
							nextToFillFloat, rowNext := b.GetNextFloat64(colIndex, rowIndex+1)
							prevRefFloat, valid2 := b.GetFloat64(refIndex, rowPrev)
							nextRefFloat, valid3 := b.GetFloat64(refIndex, rowNext)
							if valid1 && valid2 && valid3 && nextRefFloat-prevRefFloat != 0.0 {
								newBufValue[rowIndex] = (refFloat - prevRefFloat) / (nextRefFloat - prevRefFloat)
								newBufValue[rowIndex] *= (nextToFillFloat - prevToFillFloat)
								newBufValue[rowIndex] += prevToFillFloat
								newBufValid[rowIndex] = true
							}
						}
					}
				}
				seriesChannel <- Series{
					Name: colName,
					Type: typ,
					Data: Buffer{Value: newBufValue, Valid: newBufValid},
				}
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

	for colIndex, col := range b.Schema().Fields() {
		if toFill[colIndex] && b.GetType(colIndex) != Float64 && b.GetType(colIndex) != Int64 {
			err = fmt.Errorf("fill mean type error: column '%s' is of type '%s'", col.Name, b.GetType(colIndex))
			return nil, err
		}
	}

	seriesChannel := make(chan Series, b.NumCols())
	for colIndex, col := range b.Schema().Fields() {
		go func(colIndex int, colName string) {
			typ := b.GetType(colIndex)
			switch typ {
			case Int64:
				newArray := array.NewInt64Data(b.Record.Column(colIndex).Data())
				newBufValue := newArray.Int64Values()
				newBufValid := getInt64BufValid(newArray)
				if toFill[colIndex] {
					for rowIndex := 0; rowIndex < newArray.Len(); rowIndex++ {
						if !newBufValid[rowIndex] {
							prevInt, prevRow := b.GetPreviousInt64(colIndex, rowIndex-1)
							nextInt, nextRow := b.GetNextInt64(colIndex, rowIndex+1)
							if prevRow > -1 && nextRow > -1 {
								newBufValue[rowIndex] = (prevInt + nextInt) / 2
								newBufValid[rowIndex] = true
							}
						}
					}
				}
				seriesChannel <- Series{
					Name: colName,
					Type: typ,
					Data: Buffer{Value: newBufValue, Valid: newBufValid},
				}
			case Float64:
				newArray := array.NewFloat64Data(b.Record.Column(colIndex).Data())
				newBufValue := newArray.Float64Values()
				newBufValid := getFloat64BufValid(newArray)
				if toFill[colIndex] {
					for rowIndex := 0; rowIndex < newArray.Len(); rowIndex++ {
						if !newBufValid[rowIndex] {
							prevFloat, prevRow := b.GetPreviousFloat64(colIndex, rowIndex-1)
							nextFloat, nextRow := b.GetNextFloat64(colIndex, rowIndex+1)
							if prevRow > -1 && nextRow > -1 {
								newBufValue[rowIndex] = (prevFloat + nextFloat) / 2
								newBufValid[rowIndex] = true
							}
						}
					}
				}
				seriesChannel <- Series{
					Name: colName,
					Type: typ,
					Data: Buffer{Value: newBufValue, Valid: newBufValid},
				}
			}
		}(colIndex, col.Name)
	}
	return newBowFromSeriesChannel(b, seriesChannel)
}

func getInt64BufValid(a *array.Int64) []bool {
	var bools []bool

	for i := 0; i < a.Len(); i++ {
		if a.IsValid(i) {
			bools = append(bools, true)
		} else {
			bools = append(bools, false)
		}
	}
	return bools
}

func getFloat64BufValid(a *array.Float64) []bool {
	var bools []bool

	for i := 0; i < a.Len(); i++ {
		if a.IsValid(i) {
			bools = append(bools, true)
		} else {
			bools = append(bools, false)
		}
	}
	return bools
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
			switch typ {
			case Int64:
				newArray := array.NewInt64Data(b.Record.Column(colIndex).Data())
				newBufValue := newArray.Int64Values()
				newBufValid := getInt64BufValid(newArray)
				if toFill[colIndex] {
					for rowIndex := 0; rowIndex < newArray.Len(); rowIndex++ {
						if !newBufValid[rowIndex] {
							nextInt, nextRow := b.GetNextInt64(colIndex, rowIndex+1)
							if nextRow > -1 {
								newBufValue[rowIndex] = nextInt
								newBufValid[rowIndex] = true
							}
						}
					}
				}
				seriesChannel <- Series{
					Name: colName,
					Type: typ,
					Data: Buffer{Value: newBufValue, Valid: newBufValid},
				}
			case Float64:
				newArray := array.NewFloat64Data(b.Record.Column(colIndex).Data())
				newBufValue := newArray.Float64Values()
				newBufValid := getFloat64BufValid(newArray)
				if toFill[colIndex] {
					for rowIndex := 0; rowIndex < newArray.Len(); rowIndex++ {
						if !newBufValid[rowIndex] {
							nextFloat, nextRow := b.GetNextFloat64(colIndex, rowIndex+1)
							if nextRow > -1 {
								newBufValue[rowIndex] = nextFloat
								newBufValid[rowIndex] = true
							}
						}
					}
				}
				seriesChannel <- Series{
					Name: colName,
					Type: typ,
					Data: Buffer{Value: newBufValue, Valid: newBufValid},
				}
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
			switch typ {
			case Int64:
				newArray := array.NewInt64Data(b.Record.Column(colIndex).Data())
				newBufValue := newArray.Int64Values()
				newBufValid := getInt64BufValid(newArray)
				if toFill[colIndex] {
					for rowIndex := 0; rowIndex < newArray.Len(); rowIndex++ {
						if !newBufValid[rowIndex] {
							prevInt, prevRow := b.GetPreviousInt64(colIndex, rowIndex-1)
							if prevRow > -1 {
								newBufValue[rowIndex] = prevInt
								newBufValid[rowIndex] = true
							}
						}
					}
				}
				seriesChannel <- Series{
					Name: colName,
					Type: typ,
					Data: Buffer{Value: newBufValue, Valid: newBufValid},
				}
			case Float64:
				newArray := array.NewFloat64Data(b.Record.Column(colIndex).Data())
				newBufValue := newArray.Float64Values()
				newBufValid := getFloat64BufValid(newArray)
				if toFill[colIndex] {
					for rowIndex := 0; rowIndex < newArray.Len(); rowIndex++ {
						if !newBufValid[rowIndex] {
							prevFloat, prevRow := b.GetPreviousFloat64(colIndex, rowIndex-1)
							if prevRow > -1 {
								newBufValue[rowIndex] = prevFloat
								newBufValid[rowIndex] = true
							}
						}
					}
				}
				seriesChannel <- Series{
					Name: colName,
					Type: typ,
					Data: Buffer{Value: newBufValue, Valid: newBufValid},
				}
			}
		}(colIndex, col.Name)
	}
	return newBowFromSeriesChannel(b, seriesChannel)
}

// isColSorted returns nil if the column colIndex is sorted or an error otherwise.
func isColSorted(b Bow, colIndex int, typ Type) error {
	var row int
	var curr, next interface{}

	curr = b.GetValue(colIndex, row)
	if curr == nil {
		next, row = b.GetNextValue(colIndex, row+1) // skip first nil values
		if next == nil {
			return nil // empty column, column sorted
		}
	}

	if typ != Int64 && typ != Float64 {
		err := fmt.Errorf("isColSorted: type unknown")
		return err
	}

	var asc bool
	var currInt, nextInt int64
	var currFloat, nextFloat float64

	for (typ == Int64 && currInt == nextInt) ||
		(typ == Float64 && currFloat == nextFloat) { // attempt to compare first two unequal values
		curr = b.GetValue(colIndex, row)
		next, row = b.GetNextValue(colIndex, row+1)
		if next == nil {
			return nil // only one value, column sorted
		}
		if typ == Int64 {
			currInt = curr.(int64)
			nextInt = next.(int64)
			if currInt < nextInt {
				asc = true
			}
		} else if typ == Float64 {
			currFloat = curr.(float64)
			nextFloat = next.(float64)
			if currFloat < nextFloat {
				asc = true
			}
		}
		if row == b.NumRows() || row == -1 {
			return nil // only equal values, column sorted
		}
	}
	for row < b.NumRows() { // compare other values
		curr = b.GetValue(colIndex, row)
		next, row = b.GetNextValue(colIndex, row+1)
		if next == nil {
			return nil // end of values, column sorted
		}
		if typ == Int64 {
			currInt = curr.(int64)
			nextInt = next.(int64)
			if asc && currInt > nextInt {
				name, errName := b.GetName(colIndex)
				if errName != nil {
					return errName
				}
				err := fmt.Errorf("reference column '%s' is not sorted", name)
				return err
			}
		} else if typ == Float64 {
			currFloat = curr.(float64)
			nextFloat = next.(float64)
			if asc && currFloat > nextFloat {
				name, errName := b.GetName(colIndex)
				if errName != nil {
					return errName
				}
				err := fmt.Errorf("reference column '%s' is not sorted", name)
				return err
			}
		}
	}
	return nil
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
