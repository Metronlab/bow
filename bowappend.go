package bow

import (
	"fmt"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
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

	for colIndex := 0; colIndex < refBow.NumCols(); colIndex++ {
		var newArray array.Interface
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		typ := refBow.GetColType(colIndex)
		switch typ {
		case Int64:
			builder := array.NewInt64Builder(mem)
			for _, b := range bows {
				if t := b.GetColType(colIndex); t != typ {
					return nil, fmt.Errorf(
						"bow.AppendBows: incompatible types %v and %v", typ, t)
				}
				data := b.Column(colIndex).Data()
				arr := array.NewInt64Data(data)
				v := arr.Int64Values()
				valid := getValid(arr, b.NumRows())
				builder.AppendValues(v, valid)
			}
			newArray = builder.NewArray()
		case Float64:
			builder := array.NewFloat64Builder(mem)
			for _, b := range bows {
				if t := b.GetColType(colIndex); t != typ {
					return nil, fmt.Errorf(
						"bow.AppendBows: incompatible types %v and %v", typ, t)
				}
				data := b.Column(colIndex).Data()
				arr := array.NewFloat64Data(data)
				v := arr.Float64Values()
				valid := getValid(arr, b.NumRows())
				builder.AppendValues(v, valid)
			}
			newArray = builder.NewArray()
		case Bool:
			builder := array.NewBooleanBuilder(mem)
			for _, b := range bows {
				if t := b.GetColType(colIndex); t != typ {
					return nil, fmt.Errorf(
						"bow.AppendBows: incompatible types %v and %v", typ, t)
				}
				data := b.Column(colIndex).Data()
				arr := array.NewBooleanData(data)
				v := make([]bool, b.NumRows())
				for i := range v {
					v[i] = arr.Value(i)
				}
				valid := getValid(arr, b.NumRows())
				builder.AppendValues(v, valid)
			}
			newArray = builder.NewArray()
		case String:
			builder := array.NewStringBuilder(mem)
			for _, b := range bows {
				if t := b.GetColType(colIndex); t != typ {
					return nil, fmt.Errorf(
						"bow.AppendBows: incompatible types %v and %v", typ, t)
				}
				data := b.Column(colIndex).Data()
				arr := array.NewStringData(data)
				v := make([]string, b.NumRows())
				for i := range v {
					v[i] = arr.Value(i)
				}
				valid := getValid(arr, b.NumRows())
				builder.AppendValues(v, valid)
			}
			newArray = builder.NewArray()
		default:
			return nil, fmt.Errorf("bow.AppendBows: unsupported type %v", typ)
		}

		seriesSlice[colIndex] = Series{
			Name:  refBow.GetColName(colIndex),
			Array: newArray,
		}
	}

	return NewBowWithMetadata(
		Metadata{refBow.Schema().Metadata()},
		seriesSlice...)
}
