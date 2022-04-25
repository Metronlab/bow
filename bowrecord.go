package bow

import (
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
)

func newRecord(metadata Metadata, series ...Series) (arrow.Record, error) {
	var fields []arrow.Field
	var arrays []arrow.Array
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
