package bow

const (
	orderUndefined = iota
	orderASC
	orderDESC
)

// IsColSorted returns a boolean whether the column colIndex is sorted or not, skipping nil values.
// An empty column or an unsupported data type returns false.
func (b *bow) IsColSorted(colIndex int) bool {
	if b.IsColEmpty(colIndex) {
		return false
	}

	var rowIndex int
	var order = orderUndefined

	buf := b.NewBufferFromCol(colIndex)

	switch b.GetColType(colIndex) {
	case Int64:
		for !buf.Valid[rowIndex] {
			rowIndex++
		}
		curr := buf.GetValue(rowIndex).(int64)
		var next int64
		rowIndex++
		for ; rowIndex < b.NumRows(); rowIndex++ {
			if !buf.Valid[rowIndex] {
				continue
			}
			next = buf.GetValue(rowIndex).(int64)
			if order == orderUndefined {
				if curr < next {
					order = orderASC
				} else if curr > next {
					order = orderDESC
				}
			}
			if order == orderASC && next < curr ||
				order == orderDESC && next > curr {
				return false
			}
			curr = next
		}
	case Float64:
		for !buf.Valid[rowIndex] {
			rowIndex++
		}
		curr := buf.GetValue(rowIndex).(float64)
		var next float64
		rowIndex++
		for ; rowIndex < b.NumRows(); rowIndex++ {
			if !buf.Valid[rowIndex] {
				continue
			}
			next = buf.GetValue(rowIndex).(float64)
			if order == orderUndefined {
				if curr < next {
					order = orderASC
				} else if curr > next {
					order = orderDESC
				}
			}
			if order == orderASC && next < curr ||
				order == orderDESC && next > curr {
				return false
			}
			curr = next
		}
	default:
		return false
	}
	return true
}

func (b *bow) IsColEmpty(colIndex int) bool {
	var rowIndex int
	arr := b.Column(colIndex)
	for rowIndex < arr.Len() && arr.IsNull(rowIndex) {
		rowIndex++
	}
	return rowIndex == arr.Len()
}

// IsEmpty returns true if the dataframe contains no data, false otherwise.
func (b *bow) IsEmpty() bool {
	return b.NumRows() == 0
}
