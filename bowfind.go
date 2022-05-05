package bow

// Find returns the index of the row where `value` is found in the `colIndex` column.
// Returns -1 if the value is not found.
func (b *bow) Find(colIndex int, value interface{}) int {
	return b.FindNext(colIndex, 0, value)
}

// FindNext returns the index of the row where `value` is found in the `colIndex` column, starting from the `rowIndex` row.
// Returns -1 if the value is not found.
func (b *bow) FindNext(colIndex, rowIndex int, value interface{}) int {
	if value == nil {
		for i := 0; i < b.NumRows(); i++ {
			if !b.Column(colIndex).IsValid(i) {
				return i
			}
		}
		return -1
	}

	for i := rowIndex; i < b.NumRows(); i++ {
		if value == b.GetValue(colIndex, i) {
			return i
		}
	}
	return -1
}

// Contains returns whether `value` is found in `colIndex` columns.
func (b *bow) Contains(colIndex int, value interface{}) bool {
	return b.Find(colIndex, value) != -1
}
