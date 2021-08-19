// Code generated by bowbuffer.gen.go.tmpl. DO NOT EDIT.

package bow

import (
	"fmt"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/bitutil"
)

type Buffer struct {
	Data            interface{}
	nullBitmapBytes []byte
}

func NewBuffer(size int, typ Type) Buffer {
	switch typ {
	case Int64:
		return Buffer{
			Data:            make([]int64, size),
			nullBitmapBytes: make([]byte, bitutil.CeilByte(size)/8),
		}
	case Float64:
		return Buffer{
			Data:            make([]float64, size),
			nullBitmapBytes: make([]byte, bitutil.CeilByte(size)/8),
		}
	case Boolean:
		return Buffer{
			Data:            make([]bool, size),
			nullBitmapBytes: make([]byte, bitutil.CeilByte(size)/8),
		}
	case String:
		return Buffer{
			Data:            make([]string, size),
			nullBitmapBytes: make([]byte, bitutil.CeilByte(size)/8),
		}
	default:
		panic(fmt.Errorf("unsupported type %s", typ))
	}
}

func NewBufferFromData(dataArray interface{}, validityArray interface{}) Buffer {
	switch data := dataArray.(type) {
	case []int64:
		return Buffer{
			Data:            dataArray,
			nullBitmapBytes: buildNullBitmapBytes(len(data), validityArray),
		}
	case []float64:
		return Buffer{
			Data:            dataArray,
			nullBitmapBytes: buildNullBitmapBytes(len(data), validityArray),
		}
	case []bool:
		return Buffer{
			Data:            dataArray,
			nullBitmapBytes: buildNullBitmapBytes(len(data), validityArray),
		}
	case []string:
		return Buffer{
			Data:            dataArray,
			nullBitmapBytes: buildNullBitmapBytes(len(data), validityArray),
		}
	default:
		panic(fmt.Errorf("unsupported type %T", dataArray))
	}
}

func buildNullBitmapBytes(dataLength int, validityArray interface{}) []byte {
	var res []byte
	nullBitmapLength := bitutil.CeilByte(dataLength) / 8

	switch valid := validityArray.(type) {
	case nil:
		res = make([]byte, nullBitmapLength)
		for i := range res {
			bitutil.SetBit(res, i)
		}
	case []bool:
		if len(valid) != dataLength {
			panic(fmt.Errorf("dataArray and validityArray have different lengths"))
		}
		res = make([]byte, nullBitmapLength)
		for i := range res {
			if valid[i] {
				bitutil.SetBit(res, i)
			}
		}
	case []byte:
		if len(valid) != nullBitmapLength {
			panic(fmt.Errorf("dataArray and validityArray have different lengths"))
		}
		return valid
	default:
		panic(fmt.Errorf("unsupported type %T", valid))
	}

	return res
}

func NewBufferFromInterfaces(typ Type, cells []interface{}) (Buffer, error) {
	buf := NewBuffer(len(cells), typ)
	for i, c := range cells {
		buf.SetOrDrop(i, c)
	}
	return buf, nil
}

func (b *Buffer) Len() int {
	switch data := b.Data.(type) {
	case []int64:
		return len(data)
	case []float64:
		return len(data)
	case []bool:
		return len(data)
	case []string:
		return len(data)
	default:
		panic(fmt.Errorf("unsupported type '%T'", b.Data))
	}
}

func (b *Buffer) SetOrDrop(i int, value interface{}) {
	var valid bool
	switch v := b.Data.(type) {
	case []int64:
		v[i], valid = Int64.Convert(value).(int64)
	case []float64:
		v[i], valid = Float64.Convert(value).(float64)
	case []bool:
		v[i], valid = Boolean.Convert(value).(bool)
	case []string:
		v[i], valid = String.Convert(value).(string)
	default:
		panic(fmt.Errorf("unsupported type %T", v))
	}
	if valid {
		bitutil.SetBit(b.nullBitmapBytes, i)
	}
}

func (b *Buffer) IsValid(rowIndex int) bool {
	return bitutil.BitIsSet(b.nullBitmapBytes, rowIndex)
}

func (b *Buffer) IsNull(rowIndex int) bool {
	return bitutil.BitIsNotSet(b.nullBitmapBytes, rowIndex)
}

func (b *Buffer) GetValue(i int) interface{} {
	if bitutil.BitIsNotSet(b.nullBitmapBytes, i) {
		return nil
	}
	switch v := b.Data.(type) {
	case []int64:
		return v[i]
	case []float64:
		return v[i]
	case []bool:
		return v[i]
	case []string:
		return v[i]
	default:
		panic(fmt.Errorf("unsupported type %T", v))
	}
}

func (b *bow) NewBufferFromCol(colIndex int) Buffer {
	data := b.Column(colIndex).Data()
	switch b.ColumnType(colIndex) {
	case Int64:
		arr := array.NewInt64Data(data)
		return Buffer{
			Data:            Int64Values(arr),
			nullBitmapBytes: arr.NullBitmapBytes(),
		}
	case Float64:
		arr := array.NewFloat64Data(data)
		return Buffer{
			Data:            Float64Values(arr),
			nullBitmapBytes: arr.NullBitmapBytes(),
		}
	case Boolean:
		arr := array.NewBooleanData(data)
		return Buffer{
			Data:            BooleanValues(arr),
			nullBitmapBytes: arr.NullBitmapBytes(),
		}
	case String:
		arr := array.NewStringData(data)
		return Buffer{
			Data:            StringValues(arr),
			nullBitmapBytes: arr.NullBitmapBytes(),
		}
	default:
		panic(fmt.Errorf(
			"unsupported type %+v", b.ColumnType(colIndex)))
	}
}

func Int64Values(arr *array.Int64) []int64 {
	return arr.Int64Values()
}

func Float64Values(arr *array.Float64) []float64 {
	return arr.Float64Values()
}

func BooleanValues(arr *array.Boolean) []bool {
	var res = make([]bool, arr.Len())
	for i := range res {
		res[i] = arr.Value(i)
	}
	return res
}

func StringValues(arr *array.String) []string {
	var res = make([]string, arr.Len())
	for i := range res {
		res[i] = arr.Value(i)
	}
	return res
}
