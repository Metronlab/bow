package bow

import "github.com/apache/arrow/go/arrow/array"

// IsColSorted returns a boolean whether the column colIndex is sorted or not, skipping nil values.
// An empty column or an unsupported data type returns false.
func (b *bow) IsColSorted(colIndex int) bool {
	if b.IsColEmpty(colIndex) {
		return false
	}
	var rowIndex int
	var order int8

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
				if order == 0 {
					if curr > next {
						order = -1
					} else if curr < next {
						order = 1
					}
				}
				if order == -1 && next > curr || order == 1 && next < curr {
					return false
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
				if order == 0 {
					if curr > next {
						order = -1
					} else if curr < next {
						order = 1
					}
				}
				if order == -1 && next > curr || order == 1 && next < curr {
					return false
				}
				curr = next
			}
			rowIndex++
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
