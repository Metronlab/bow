package bow

import (
	"errors"
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

type Series2 struct {
	Name  string
	Array array.Interface
}

func NewSeries2(name string, data *array.Data) Series2 {
	return Series2{
		Name:  name,
		Array: array.MakeFromData(data),
	}
}

func newRecordFromSeries2(series ...Series2) (array.Record, error) {
	if len(series) == 0 {
		return nil, nil
	}

	var fields []arrow.Field
	var cols []array.Interface
	var nrows int
	for _, s := range series {
		if s.Name == "" {
			return nil, errors.New("bow: empty series name")
		}
		field := arrow.Field{Name: s.Name}
		if getTypeFromArrowType(s.Array.DataType()) == Unknown {
			return nil, fmt.Errorf("bow: unhandled type: %s", s.Array.DataType().Name())
		}
		field.Type = s.Array.DataType()
		fields = append(fields, field)
		cols = append(cols, s.Array)
		nrows = s.Array.Len()
	}
	schema := arrow.NewSchema(fields, nil)
	return array.NewRecord(schema, cols, int64(nrows)), nil
}
