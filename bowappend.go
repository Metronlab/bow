package bow

import (
	"fmt"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
)

// AppendBows attempts to append bows with equal schemas.
// Different schemas will lead to undefined behavior.
// Resulting metadata is copied from the first bow.
func AppendBows(bows ...Bow) (Bow, error) {
	if len(bows) == 0 {
		return nil, nil
	}

	if len(bows) == 1 {
		return bows[0], nil
	}

	numRows := 0
	for _, b := range bows {
		numRows += b.NumRows()
	}

	refBow := bows[0]
	seriesSlice := make([]Series, refBow.NumCols())

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	for colIndex := 0; colIndex < refBow.NumCols(); colIndex++ {
		var newArray arrow.Array
		refType := refBow.ColumnType(colIndex)
		switch refType {
		case Int64:
			builder := array.NewInt64Builder(mem)
			builder.Resize(numRows)
			for _, b := range bows {
				if colType := b.ColumnType(colIndex); colType != refType {
					return nil, fmt.Errorf(
						"bow.AppendBows: incompatible types %v and %v", refType, colType)
				}
				data := b.(*bow).Column(colIndex).Data()
				arr := array.NewInt64Data(data)
				v := Int64Values(arr)
				valid := getValiditySlice(arr)
				builder.AppendValues(v, valid)
			}
			newArray = builder.NewArray()
		case Float64:
			builder := array.NewFloat64Builder(mem)
			builder.Resize(numRows)
			for _, b := range bows {
				if colType := b.ColumnType(colIndex); colType != refType {
					return nil, fmt.Errorf(
						"bow.AppendBows: incompatible types %v and %v", refType, colType)
				}
				data := b.(*bow).Column(colIndex).Data()
				arr := array.NewFloat64Data(data)
				v := Float64Values(arr)
				valid := getValiditySlice(arr)
				builder.AppendValues(v, valid)
			}
			newArray = builder.NewArray()
		case Bool:
			builder := array.NewBooleanBuilder(mem)
			builder.Resize(numRows)
			for _, b := range bows {
				if colType := b.ColumnType(colIndex); colType != refType {
					return nil, fmt.Errorf(
						"bow.AppendBows: incompatible types %v and %v", refType, colType)
				}
				data := b.(*bow).Column(colIndex).Data()
				arr := array.NewBooleanData(data)
				v := BooleanValues(arr)
				valid := getValiditySlice(arr)
				builder.AppendValues(v, valid)
			}
			newArray = builder.NewArray()
		case String:
			builder := array.NewStringBuilder(mem)
			builder.Resize(numRows)
			for _, b := range bows {
				if colType := b.ColumnType(colIndex); colType != refType {
					return nil, fmt.Errorf(
						"bow.AppendBows: incompatible types %v and %v", refType, colType)
				}
				data := b.(*bow).Column(colIndex).Data()
				arr := array.NewStringData(data)
				v := StringValues(arr)
				valid := getValiditySlice(arr)
				builder.AppendValues(v, valid)
			}
			newArray = builder.NewArray()
		default:
			return nil, fmt.Errorf("unsupported type %v", refType)
		}

		seriesSlice[colIndex] = Series{
			Name:  refBow.ColumnName(colIndex),
			Array: newArray,
		}
	}

	return NewBowWithMetadata(refBow.Metadata(), seriesSlice...)
}
