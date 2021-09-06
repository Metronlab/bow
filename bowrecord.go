package bow

import (
	"errors"
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

func NewFromRecord(record array.Record) (Bow, error) {
	for _, f := range record.Schema().Fields() {
		if getBowTypeFromArrowType(f.Type) == Unknown {
			return nil, fmt.Errorf("unsupported type: %s", f.Type.Name())
		}
	}
	return &bow{Record: record}, nil
}

func newRecord(metadata Metadata, series ...Series) (array.Record, error) {
	var fields []arrow.Field
	var arrays []array.Interface
	var nRows int64

	if len(series) != 0 && series[0].Array != nil {
		nRows = int64(series[0].Array.Len())
	}

	for _, s := range series {
		if s.Array == nil {
			return nil, errors.New("empty Series")
		}
		if s.Name == "" {
			return nil, errors.New("empty Series name")
		}
		if getBowTypeFromArrowType(s.Array.DataType()) == Unknown {
			return nil, fmt.Errorf("unsupported type: %s", s.Array.DataType().Name())
		}
		if int64(s.Array.Len()) != nRows {
			return nil,
				fmt.Errorf(
					"bow.Series '%s' has a length of %d, which is different from the previous ones",
					s.Name, s.Array.Len())
		}
		fields = append(fields, arrow.Field{Name: s.Name, Type: s.Array.DataType()})
		arrays = append(arrays, s.Array)
	}

	return array.NewRecord(
		arrow.NewSchema(fields, &metadata.Metadata),
		arrays, nRows), nil
}
