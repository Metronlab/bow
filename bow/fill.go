package bow

import "fmt"

// FillLinear fills any row that contains a nil for any of `nilCols`
// using Linear Interpolation method and returns a new Bow.
// (`colNames` defaults to all columns)
func (b *bow) FillLinear(colNames ...string) (bobow Bow, err error) {
	toFill, err := colsToFill(b, colNames)
	if err != nil {
		return nil, err
	}

	for colIndex, col := range b.Schema().Fields() {
		if toFill[colIndex] && b.GetType(colIndex) != Float64 && b.GetType(colIndex) != Int64 {
			err = fmt.Errorf("linear interpolation type error: column '%s' is of type '%s'", col.Name, b.GetType(colIndex))
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
func (b *bow) FillNext(colNames ...string) (bobow Bow, err error) {
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
func (b *bow) FillPrevious(colNames ...string) (bobow Bow, err error) {
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

// colsToFill returns a bool slice of size b.NumCols
// with 'true' values at indexes of the corresponding colNames
// (`colNames` defaults to all columns)
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

func newBowFromSeriesChannel(b *bow, seriesChannel chan Series) (bobow Bow, err error) {
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
