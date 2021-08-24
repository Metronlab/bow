package bow

func (b *bow) Find(columnIndex int, value interface{}) int {
	return b.FindNext(columnIndex, 0, value)
}

func (b *bow) FindNext(columnIndex, rowIndex int, value interface{}) int {
	if value == nil {
		col := b.Column(columnIndex)
		for i := 0; i < b.NumRows(); i++ {
			if !col.IsValid(i) {
				return i
			}
		}
		return -1
	}

	for i := rowIndex; i < b.NumRows(); i++ {
		if value == b.GetValue(columnIndex, i) {
			return i
		}
	}
	return -1
}

func (b *bow) Contains(columnIndex int, value interface{}) bool {
	return b.Find(columnIndex, value) != -1
}
