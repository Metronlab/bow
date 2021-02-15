package bow

import (
	"fmt"
	"github.com/apache/arrow/go/arrow/array"
)

const (
	OrderUndefined = iota
	OrderASC
	OrderDESC
)

// IsColSorted returns a boolean whether the column colIndex is sorted or not, skipping nil values.
// An empty column or an unsupported data type returns false.
func (b *bow) IsColSorted(colIndex int) (bool, error) {
	empty, err := b.IsColEmpty(colIndex)
	if err != nil {
		return false, fmt.Errorf("IsColSorted: %w", err)
	}
	if empty {
		return false, nil
	}
	var rowIndex int
	var order = OrderUndefined

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
		for rowIndex < len(values) {
			if arr.IsValid(rowIndex) {
				next = values[rowIndex]
				if order == OrderUndefined {
					if curr < next {
						order = OrderASC
					} else if curr > next {
						order = OrderDESC
					}
				}
				if order == OrderASC && next < curr ||
					order == OrderDESC && next > curr {
					return false, nil
				}
				curr = next
			}
			rowIndex++
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
		for rowIndex < len(values) {
			if arr.IsValid(rowIndex) {
				next = values[rowIndex]
				if order == OrderUndefined {
					if curr < next {
						order = OrderASC
					} else if curr > next {
						order = OrderDESC
					}
				}
				if order == OrderASC && next < curr ||
					order == OrderDESC && next > curr {
					return false, nil
				}
				curr = next
			}
			rowIndex++
		}
	default:
		return false, nil
	}
	return true, nil
}

func (b *bow) IsColEmpty(colIndex int) (bool, error) {
	if _, err := b.GetName(colIndex); err != nil {
		return false, fmt.Errorf("IsColEmpty: invalid colIndex: %w", err)
	}

	var rowIndex int
	arr := b.Column(colIndex)
	for rowIndex < arr.Len() && arr.IsNull(rowIndex) {
		rowIndex++
	}
	return rowIndex == arr.Len(), nil
}

// IsEmpty returns true if the dataframe contains no data, false otherwise.
func (b *bow) IsEmpty() bool {
	return b.NumRows() == 0
}
