package bow

import (
	"github.com/apache/arrow/go/arrow/array"
)

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

	switch b.GetType(colIndex) {
	case Int64:
		arr := array.NewInt64Data(b.Record.Column(colIndex).Data())
		values := arr.Int64Values()
		for arr.IsNull(rowIndex) {
			rowIndex++
		}
		curr := values[rowIndex]
		var next int64
		rowIndex++
		for ; rowIndex < len(values); rowIndex++ {
			if !arr.IsValid(rowIndex) {
				continue
			}
			next = values[rowIndex]
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
		arr := array.NewFloat64Data(b.Record.Column(colIndex).Data())
		values := arr.Float64Values()
		for arr.IsNull(rowIndex) {
			rowIndex++
		}
		curr := values[rowIndex]
		var next float64
		rowIndex++
		for ; rowIndex < len(values); rowIndex++ {
			if !arr.IsValid(rowIndex) {
				continue
			}
			next = values[rowIndex]
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
