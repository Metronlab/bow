package bow

import (
	"errors"
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"reflect"
	"time"
)

func makeBowFromMaps(gms []GenericMap) (Bow, error) {
	if len(gms) == 0 {
		return nil, errors.New("empty array")
	}
	schema, err := newSchemaFromMap(gms[0])
	if err != nil {
		return nil, err
	}

	buffer, err := newFactoryBuffer(schema, gms[0], len(gms))
	if err != nil {
		return nil, err
	}

	if err = buffer.fill(schema, gms); err != nil {
		return nil, err
	}

	r, err := buffer.getNewRecord(schema)
	if err != nil {
		return nil, err
	}

	return &bow{
		Record: r,
	}, nil
}

type colBuffer struct {
	Value interface{}
	Valid []bool
}

type factoryBuffer []colBuffer

func newFactoryBuffer(schema *arrow.Schema, genericMap GenericMap, bufLen int) (factoryBuffer, error) {
	fieldsBuf := make(factoryBuffer, len(schema.Fields()))
	for i := range fieldsBuf {
		switch genericMap[schema.Field(i).Name].(type){
		case float64:
			fieldsBuf[i] = colBuffer{
				Value: make([]float64, bufLen),
				Valid: make([]bool, bufLen),
			}
		case int64:
			fieldsBuf[i] = colBuffer{
				Value: make([]int64, bufLen),
				Valid: make([]bool, bufLen),
			}
		case time.Time:
			fieldsBuf[i] = colBuffer{
				Value: make([]arrow.Time32, bufLen),
				Valid: make([]bool, bufLen),
			}
		case bool:
			fieldsBuf[i] = colBuffer{
				Value: make([]bool, bufLen),
				Valid: make([]bool, bufLen),
			}
		default:
			return nil, fmt.Errorf("bow: unsuported type: %v",
					reflect.TypeOf(genericMap[schema.Field(i).Name]))
		}
	}
	return fieldsBuf, nil
}

func (f factoryBuffer) fill(schema *arrow.Schema, genericMaps []GenericMap) error {
	if len(f) != len(genericMaps) {
		return fmt.Errorf("bow: inconsistancy between buffer and map len: %d vs %d",
			len(f), len(genericMaps))
	}
	for rowIndex, genericMap := range genericMaps {
		for colIndex := range schema.Fields() {
			switch f[0].Value.(type) {
			case []float64:
				f[colIndex].Value, f[colIndex].Valid[rowIndex] =
					genericMap.getFloat64Value(schema.Field(colIndex).Name)
			case []int64:
				f[colIndex].Value, f[colIndex].Valid[rowIndex] =
					genericMap.getInt64Value(schema.Field(colIndex).Name)
			case []bool:
				f[colIndex].Value, f[colIndex].Valid[rowIndex] =
					genericMap.getBoolValue(schema.Field(colIndex).Name)
			case []arrow.Time32:
				f[colIndex].Value, f[colIndex].Valid[rowIndex] =
					genericMap.getTimeMsValue(schema.Field(colIndex).Name)
			default:
				panic("bow: factory buffers type must be known in fill")
			}
		}
	}
	return nil
}

func (f factoryBuffer) getNewRecord(schema *arrow.Schema) (array.Record, error) {
	if len(f) == 0 {
		return nil, errors.New("bow: empty buffer cannot return record")
	}
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	for colIndex := range f {
		switch f[colIndex].Value.(type) {
		case []float64:
			b.Field(colIndex).(*array.Float64Builder).
				AppendValues(f[colIndex].Value.([]float64), f[colIndex].Valid)
		case []int64:
			b.Field(colIndex).(*array.Int64Builder).
				AppendValues(f[colIndex].Value.([]int64), f[colIndex].Valid)
		case []bool:
			b.Field(colIndex).(*array.BooleanBuilder).
				AppendValues(f[colIndex].Value.([]bool), f[colIndex].Valid)
		case []arrow.Time32:
			b.Field(colIndex).(*array.Time32Builder).
				AppendValues(f[colIndex].Value.([]arrow.Time32), f[colIndex].Valid)
		}
	}

	return b.NewRecord(), nil
}
