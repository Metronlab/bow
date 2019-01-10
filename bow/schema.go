package bow

import (
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"reflect"
	"time"
)

func newSchemaFromMap(genericMap GenericMap) (*arrow.Schema, error) {
	var fields []arrow.Field
	for k, v := range genericMap {
		field := arrow.Field{Name: k}

		switch v.(type) {
		case float64: field.Type = arrow.PrimitiveTypes.Float64
		case int64: field.Type = arrow.PrimitiveTypes.Int64
		case time.Time: field.Type = arrow.FixedWidthTypes.Time32ms
		case bool: field.Type = arrow.FixedWidthTypes.Boolean
		default:
			return nil,
				fmt.Errorf("bow: unsuported type: %v", reflect.TypeOf(v))
		}

		fields = append(fields, field)
	}

	return arrow.NewSchema(fields, nil), nil
}