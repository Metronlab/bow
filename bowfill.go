package bow

import "fmt"

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
			buf := NewBuffer(b.NumRows(), typ, true)
			for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
				newValue := b.GetValue(colIndex, rowIndex)
				if newValue == nil && colIndex == toFillIndex {
					prevCol, prevRow := b.GetPreviousValue(colIndex, rowIndex)
					nextCol, nextRow := b.GetNextValue(colIndex, rowIndex)
					ref := b.GetValue(refIndex, rowIndex)
					prevRef, _ := b.GetPreviousValue(refIndex, prevRow)
					nextRef, _ := b.GetNextValue(refIndex, nextRow)
					if prevCol != nil && nextCol != nil && ref != nil && prevRef != nil && nextRef != nil {
						if typ == Float64 {
							prevFloat, _ := ToFloat64(prevCol)
							nextFloat, _ := ToFloat64(nextCol)
							refFloat, _ := ToFloat64(ref)
							prevRefFloat, _ := ToFloat64(prevRef)
							nextRefFloat, _ := ToFloat64(nextRef)
							newValue = ((refFloat-prevRefFloat)/(nextRefFloat-prevRefFloat))*(nextFloat-prevFloat) + prevFloat
						} else if typ == Int64 {
							prevInt, _ := ToInt64(prevCol)
							nextInt, _ := ToInt64(nextCol)
							refInt, _ := ToInt64(ref)
							prevRefInt, _ := ToInt64(prevRef)
							nextRefInt, _ := ToInt64(nextRef)
							newValue = ((refInt-prevRefInt)/(nextRefInt-prevRefInt))*(nextInt-prevInt) + prevInt
						}
					}
				}
				buf.SetOrDrop(rowIndex, newValue)
			}
			seriesChannel <- Series{Name: colName, Type: typ, Data: buf}
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
			buf := NewBuffer(b.NumRows(), typ, true)
			for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
				newValue := b.GetValue(colIndex, rowIndex)
				if newValue == nil && toFill[colIndex] {
					prev, _ := b.GetPreviousValue(colIndex, rowIndex)
					next, _ := b.GetNextValue(colIndex, rowIndex)
					if prev != nil && next != nil {
						if typ == Float64 {
							var prevFloat, nextFloat float64
							prevFloat, _ = ToFloat64(prev)
							nextFloat, _ = ToFloat64(next)
							newValue = (prevFloat + nextFloat) / 2
						} else if typ == Int64 {
							var prevInt, nextInt int64
							prevInt, _ = ToInt64(prev)
							nextInt, _ = ToInt64(next)
							newValue = (prevInt + nextInt) / 2
						}
					}
				}
				buf.SetOrDrop(rowIndex, newValue)
			}
			seriesChannel <- Series{Name: colName, Type: typ, Data: buf}
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
			buf := NewBuffer(b.NumRows(), typ, true)
			for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
				newValue := b.GetValue(colIndex, rowIndex)
				if newValue == nil && toFill[colIndex] {
					newValue, _ = b.GetNextValue(colIndex, rowIndex)
				}
				buf.SetOrDrop(rowIndex, newValue)
			}
			seriesChannel <- Series{Name: colName, Type: typ, Data: buf}
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
			buf := NewBuffer(b.NumRows(), typ, true)
			for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
				newValue := b.GetValue(colIndex, rowIndex)
				if newValue == nil && toFill[colIndex] {
					newValue, _ = b.GetPreviousValue(colIndex, rowIndex)
				}
				buf.SetOrDrop(rowIndex, newValue)
			}
			seriesChannel <- Series{Name: colName, Type: typ, Data: buf}
		}(colIndex, col.Name)
	}
	return newBowFromSeriesChannel(b, seriesChannel)
}

func (b *bow) FillPreviousNoConcurrency(colNames ...string) (Bow, error) {
	toFill, err := colsToFill(b, colNames)
	if err != nil {
		return nil, err
	}

	series := make([]Series, b.NumCols())
	for colIndex, col := range b.Schema().Fields() {
		typ := b.GetType(colIndex)
		buf := NewBuffer(b.NumRows(), typ, true)
		for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
			newValue := b.GetValue(colIndex, rowIndex)
			if newValue == nil && toFill[colIndex] {
				newValue, _ = b.GetPreviousValue(colIndex, rowIndex)
			}
			buf.SetOrDrop(rowIndex, newValue)
		}
		series[colIndex] = Series{Name: col.Name, Type: typ, Data: buf}
	}
	return NewBow(series...)
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

	var asc bool
	if typ == Int64 {
		var currInt, nextInt int64

		for currInt == nextInt { // attempt to compare first two unequal values
			curr = b.GetValue(colIndex, row)
			next, row = b.GetNextValue(colIndex, row+1)
			if next == nil {
				return nil // only one value, column sorted
			}
			currInt = curr.(int64)
			nextInt = next.(int64)
			if currInt < nextInt {
				asc = true
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
		}
	} else if typ == Float64 {
		var currFloat, nextFloat float64

		for currFloat == nextFloat { // attempt to compare first two unequal values
			curr = b.GetValue(colIndex, row)
			next, row = b.GetNextValue(colIndex, row+1)
			if next == nil {
				return nil // only one value, column sorted
			}
			currFloat = curr.(float64)
			nextFloat = next.(float64)
			if currFloat < nextFloat {
				asc = true
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
			if row == b.NumRows() || row == -1 {
				return nil // only equal values, column sorted
			}
		}
	} else {
		err := fmt.Errorf("isColSorted: type unknown")
		return err
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
