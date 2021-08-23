package bow

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
)

func newRecordFromArrays(metadata Metadata, colNames []string, arrays []array.Interface) (array.Record, error) {
	var fields []arrow.Field
	var nRows int64

	if len(arrays) != 0 {
		nRows = int64(arrays[0].Len())
	}

	if len(arrays) != len(colNames) {
		return nil, fmt.Errorf(
			"bow.newRecordFromArrays: arrays (%d) and colNames (%d) lengths are different",
			len(arrays), len(colNames))
	}

	for i, arr := range arrays {
		if int64(arr.Len()) != nRows {
			return nil,
				fmt.Errorf(
					"bow.newRecordFromArrays: array %d has a length of %d, which is different from the previous ones",
					i, arr.Len())
		}

		fields = append(fields, arrow.Field{Name: colNames[i], Type: arr.DataType()})
	}

	return array.NewRecord(
		arrow.NewSchema(fields, &metadata.Metadata),
		arrays, nRows), nil
}

func newRecordFromSeries(metadata Metadata, seriesSlice ...Series) (array.Record, error) {
	var fields []arrow.Field
	var arrays []array.Interface
	var nRows int64

	if len(seriesSlice) != 0 {
		nRows = int64(seriesSlice[0].Len())
	}

	for _, s := range seriesSlice {
		if s.Name == "" {
			return nil, errors.New("bow.newRecordFromSeries: empty Series name")
		}

		if int64(s.Len()) != nRows {
			return nil,
				fmt.Errorf(
					"bow.newRecordFromSeries: Series '%s' has a length of %d, which is different from the previous ones",
					s.Name, s.Len())
		}

		typ := s.DataType()

		arrowType, ok := mapBowToArrowTypes[typ]
		if !ok {
			panic(fmt.Errorf(
				"bow.newRecordFromSeries: typ is %v, but have %v",
				typ, reflect.TypeOf(s.Data)))
		}
		fields = append(fields, arrow.Field{Name: s.Name, Type: arrowType})

		var arr array.Interface
		switch typ {
		case Int64:
			data, ok := s.Data.([]int64)
			if !ok {
				panic(fmt.Errorf(
					"bow.newRecordFromSeries: typ is %v, but have %v",
					typ, reflect.TypeOf(s.Data)))
			}
			arr = newInt64Array(data, buildNullBitmapBytes(len(data), s.nullBitmapBytes))
		case Float64:
			data, ok := s.Data.([]float64)
			if !ok {
				panic(fmt.Errorf(
					"bow.newRecordFromSeries: typ is %v, but have %v",
					typ, reflect.TypeOf(s.Data)))
			}
			arr = newFloat64Array(data, buildNullBitmapBytes(len(data), s.nullBitmapBytes))
		case Boolean:
			data, ok := s.Data.([]bool)
			if !ok {
				panic(fmt.Errorf(
					"bow.newRecordFromSeries: typ is %v, but have %v",
					typ, reflect.TypeOf(s.Data)))
			}
			arr = newBooleanArray(data,
				buildNullBitmapBytes(len(data), s.nullBitmapBytes))
		case String:
			data, ok := s.Data.([]string)
			if !ok {
				panic(fmt.Errorf(
					"bow.newRecordFromSeries: typ is %v, but have %v",
					typ, reflect.TypeOf(s.Data)))
			}
			arr = newStringArray(data, buildNullBitmapBytes(len(data), s.nullBitmapBytes))
		default:
			panic(fmt.Errorf("unsupported type %v", typ))
		}

		arrays = append(arrays, arr)
	}

	return array.NewRecord(
		arrow.NewSchema(fields, &metadata.Metadata),
		arrays, nRows), nil
}

func buildNullBitmapBytes(dataLength int, validityArray interface{}) []byte {
	var res []byte
	nullBitmapLength := bitutil.CeilByte(dataLength) / 8

	switch valid := validityArray.(type) {
	case nil:
		res = make([]byte, nullBitmapLength)
		for i := 0; i < dataLength; i++ {
			bitutil.SetBit(res, i)
		}
	case []bool:
		if len(valid) != dataLength {
			panic(fmt.Errorf("dataArray and validityArray have different lengths"))
		}
		res = make([]byte, nullBitmapLength)
		for i := 0; i < dataLength; i++ {
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

func newInt64Array(data []int64, valid []byte) array.Interface {
	length := len(data)
	return array.NewInt64Data(
		array.NewData(arrow.PrimitiveTypes.Int64, length,
			[]*memory.Buffer{
				memory.NewBufferBytes(valid),
				memory.NewBufferBytes(arrow.Int64Traits.CastToBytes(data)),
			}, nil, length-bitutil.CountSetBits(valid, 0, length), 0))
}

func newFloat64Array(data []float64, valid []byte) array.Interface {
	length := len(data)
	return array.NewFloat64Data(
		array.NewData(arrow.PrimitiveTypes.Float64, length,
			[]*memory.Buffer{
				memory.NewBufferBytes(valid),
				memory.NewBufferBytes(arrow.Float64Traits.CastToBytes(data)),
			}, nil, length-bitutil.CountSetBits(valid, 0, length), 0))
}

func newBooleanArray(data []bool, valid []byte) array.Interface {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	builder := array.NewBooleanBuilder(mem)
	defer builder.Release()
	builder.AppendValues(data,
		buildNullBitmapBool(len(data), valid))
	return builder.NewArray()
}

func newStringArray(data []string, valid []byte) array.Interface {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	builder := array.NewStringBuilder(mem)
	defer builder.Release()
	builder.AppendValues(data,
		buildNullBitmapBool(len(data), valid))
	return builder.NewArray()
}

func buildNullBitmapBool(dataLength int, validityArray interface{}) []bool {
	switch valid := validityArray.(type) {
	case nil:
		return nil
	case []bool:
		if len(valid) != dataLength {
			panic(fmt.Errorf("dataArray and validityArray have different lengths"))
		}
		return valid
	case []byte:
		if len(valid) != bitutil.CeilByte(dataLength)/8 {
			panic(fmt.Errorf("dataArray and validityArray have different lengths"))
		}
		res := make([]bool, dataLength)
		for i := 0; i < dataLength; i++ {
			if bitutil.BitIsSet(valid, i) {
				res[i] = true
			}
		}
		return res
	default:
		panic(fmt.Errorf("unsupported type %T", valid))
	}
}
