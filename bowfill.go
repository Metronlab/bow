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
					prevCol, prevIndex := b.GetPreviousValue(colIndex, rowIndex)
					nextCol, nextIndex := b.GetNextValue(colIndex, rowIndex)
					ref := b.GetValue(refIndex, rowIndex)
					prevRef, prevRefIndex := b.GetPreviousValue(refIndex, prevIndex)
					nextRef, nextRefIndex := b.GetNextValue(refIndex, nextIndex)
					if prevCol != nil && nextCol != nil && ref != nil && prevRef != nil && nextRef != nil {
						if typ == Float64 {
							prevVal, ok := ToFloat64(prevCol)
							if !ok {
								panic(fmt.Errorf("fill linear convert error: row %d column '%s'", prevIndex, colName))
							}
							nextVal, ok := ToFloat64(nextCol)
							if !ok {
								panic(fmt.Errorf("fill linear convert error: row %d column '%s'", nextIndex, colName))
							}
							refFloat, ok := ToFloat64(ref)
							if !ok {
								panic(fmt.Errorf("fill linear convert error: row %d column '%s'", refIndex, colName))
							}
							prevRefFloat, ok := ToFloat64(prevRef)
							if !ok {
								panic(fmt.Errorf("fill linear convert error: row %d column '%s'", prevRefIndex, colName))
							}
							nextRefFloat, ok := ToFloat64(nextRef)
							if !ok {
								panic(fmt.Errorf("fill linear convert error: row %d column '%s'", nextRefIndex, colName))
							}
							newValue = ((refFloat-prevRefFloat)/(nextRefFloat-prevRefFloat))*(nextVal-prevVal) + prevVal
						} else if typ == Int64 {
							prevVal, ok := ToInt64(prevCol)
							if !ok {
								panic(fmt.Errorf("fill linear convert error: row %d column '%s'", prevIndex, colName))
							}
							nextVal, ok := ToInt64(nextCol)
							if !ok {
								panic(fmt.Errorf("fill linear convert error: row %d column '%s'", nextIndex, colName))
							}
							refInt, ok := ToInt64(ref)
							if !ok {
								panic(fmt.Errorf("fill linear convert error: row %d column '%s'", refIndex, colName))
							}
							prevRefInt, ok := ToInt64(prevRef)
							if !ok {
								panic(fmt.Errorf("fill linear convert error: row %d column '%s'", prevRefIndex, colName))
							}
							nextRefInt, ok := ToInt64(nextRef)
							if !ok {
								panic(fmt.Errorf("fill linear convert error: row %d column '%s'", nextRefIndex, colName))
							}
							newValue = ((refInt-prevRefInt)/(nextRefInt-prevRefInt))*(nextVal-prevVal) + prevVal
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
					prev, prevIndex := b.GetPreviousValue(colIndex, rowIndex)
					next, nextIndex := b.GetNextValue(colIndex, rowIndex)
					if prev != nil && next != nil {
						var ok bool
						if typ == Float64 {
							var prevVal, nextVal float64
							prevVal, ok = ToFloat64(prev)
							if !ok {
								panic(fmt.Errorf("fill mean convert error: row %d column '%s'", prevIndex, colName))
							}
							nextVal, ok = ToFloat64(next)
							if !ok {
								panic(fmt.Errorf("fill mean convert error: row %d column '%s'", nextIndex, colName))
							}
							newValue = (prevVal + nextVal) / 2
						} else if typ == Int64 {
							var prevVal, nextVal int64
							prevVal, ok = ToInt64(prev)
							if !ok {
								panic(fmt.Errorf("fill mean convert error: row %d column '%s'", prevIndex, colName))
							}
							nextVal, ok = ToInt64(next)
							if !ok {
								panic(fmt.Errorf("fill mean convert error: row %d column '%s'", nextIndex, colName))
							}
							newValue = (prevVal + nextVal) / 2
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
