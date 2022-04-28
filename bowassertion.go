package bow

import (
	"github.com/apache/arrow/go/v8/arrow/array"
)

const (
	orderUndefined = iota
	orderASC
	orderDESC
)

// IsColSorted returns a boolean whether the column colIndex is sorted or not, skipping nil values.
// An empty column or an unsupported data type returns false.
// Supports only Int64 and Float64.
func (b *bow) IsColSorted(colIndex int) bool {
	if b.IsColEmpty(colIndex) {
		return false
	}
	var rowIndex int
	var order = orderUndefined

	switch b.ColumnType(colIndex) {
	case Int64:
		arr := array.NewInt64Data(b.Column(colIndex).Data())
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
		arr := array.NewFloat64Data(b.Column(colIndex).Data())
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

// IsColEmpty returns false if the column has at least one non-nil value, and true otherwise.
func (b *bow) IsColEmpty(colIndex int) bool {
	return b.Column(colIndex).NullN() == b.Column(colIndex).Len()
}
