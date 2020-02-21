package bow

// FillPrevious fills any row that contains a nil for any of `nilCols`
// using LOCF (Last Obs. Carried Forward) method and returns a new Bow.
// (`nilCols` defaults to all columns)
func (b *bow) FillPrevious(nilCols ...string) (Bow, error) {
	toInterpolate := make([]bool, len(b.Schema().Fields()))
	nilColsNb := len(nilCols)
	for i := 0; i < nilColsNb; i++ {
		foundColIndex, err := b.GetIndex(nilCols[i])
		if err != nil {
			return nil, err
		}
		toInterpolate[foundColIndex] = true
	}

	filledSeries := make([]Series, len(b.Schema().Fields()))
	for colIndex, col := range b.Schema().Fields() {
		typ := b.GetType(colIndex)
		buf := NewBuffer(b.NumRows(), typ, true)
		for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
			if (toInterpolate[colIndex] || nilColsNb == 0) && b.GetValue(colIndex, rowIndex) == nil {
				value, _ := b.GetPreviousValue(colIndex, rowIndex)
				buf.SetOrDrop(rowIndex, value)
			} else {
				buf.SetOrDrop(rowIndex, b.GetValue(colIndex, rowIndex))
			}
		}
		filledSeries[colIndex] = Series{
			Name: col.Name,
			Type: typ,
			Data: buf,
		}
	}
	return NewBow(filledSeries...)
}
