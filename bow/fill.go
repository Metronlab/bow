package bow

// colsToFill returns a bool slice of size b.NumCols
// with 'true' values at indexes of the corresponding colNames
// (`colNames` defaults to all columns)
func colsToFill(b *bow, colNames []string) ([]bool, error) {
	toFill := make([]bool, b.NumCols())
	nilColsNb := len(colNames)
	// default: all columns to fill
	if nilColsNb == 0 {
		for i := 0; i < b.NumCols(); i++ {
			toFill[i] = true
		}
	} else {
		for i := 0; i < nilColsNb; i++ {
			foundColIndex, err := b.GetIndex(colNames[i])
			if err != nil {
				return nil, err
			}
			toFill[foundColIndex] = true
		}
	}
	return toFill, nil
}

// FillPrevious fills any row that contains a nil for any of `nilCols`
// using LOCF (Last Obs. Carried Forward) method and returns a new Bow.
// (`colNames` defaults to all columns)
func (b *bow) FillPrevious(colNames ...string) (Bow, error) {
	toFill, err := colsToFill(b, colNames)
	if err != nil {
		return nil, err
	}
	filledSeries := make([]Series, b.NumCols())
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
		filledSeries[colIndex] = Series{
			Name: col.Name,
			Type: typ,
			Data: buf,
		}
	}
	return NewBow(filledSeries...)
}
