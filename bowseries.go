package bow

import (
	"errors"
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

type Series struct {
	Name string
	Type Type
	Data Buffer
}

func NewSeries(name string, t Type, dataArray interface{}, validArray []bool) Series {
	return Series{
		Name: name,
		Type: t,
		Data: Buffer{
			Value: dataArray,
			Valid: validArray,
		},
	}
}

func NewSeriesFromInterfaces(name string, typeOf Type, cells []interface{}) (series Series, err error) {
	if typeOf == Unknown {
		if typeOf, err = seekType(cells); err != nil {
			return
		}
	}
	buf, err := NewBufferFromInterfaces(typeOf, cells)
	if err != nil {
		return Series{}, err
	}
	return NewSeries(name, typeOf, buf.Value, buf.Valid), nil
}

func newRecordFromSeries(series ...Series) (array.Record, error) {
	if len(series) == 0 {
		return nil, nil
	}

	var fields []arrow.Field

	for _, s := range series {
		if s.Name == "" {
			return nil, errors.New("bow: empty series name")
		}
		field := arrow.Field{Name: s.Name}

		var ok bool
		field.Type, ok = s.Type.arrowDataType()
		if !ok {
			return nil, fmt.Errorf("bow: unhandled type: %s", s.Type)
		}

		fields = append(fields, field)
	}

	schema := arrow.NewSchema(fields, nil)

	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	for colIndex, s := range series {
		switch s.Type {
		case Float64:
			if len(s.Data.Value.([]float64)) == 0 {
				return b.NewRecord(), nil
			}
			b.Field(colIndex).(*array.Float64Builder).
				AppendValues(s.Data.Value.([]float64), s.Data.Valid)
		case Int64:
			if len(s.Data.Value.([]int64)) == 0 {
				return b.NewRecord(), nil
			}
			b.Field(colIndex).(*array.Int64Builder).
				AppendValues(s.Data.Value.([]int64), s.Data.Valid)
		case Bool:
			if len(s.Data.Value.([]bool)) == 0 {
				return b.NewRecord(), nil
			}
			b.Field(colIndex).(*array.BooleanBuilder).
				AppendValues(s.Data.Value.([]bool), s.Data.Valid)
		case String:
			if len(s.Data.Value.([]string)) == 0 {
				return b.NewRecord(), nil
			}
			b.Field(colIndex).(*array.StringBuilder).
				AppendValues(s.Data.Value.([]string), s.Data.Valid)
		}
	}

	return b.NewRecord(), nil
}
