package bow

import (
	"encoding/json"

	"github.com/apache/arrow/go/arrow/bitutil"
)

type Series struct {
	Name            string
	Data            interface{}
	nullBitmapBytes []byte
}

func (s *Series) IsValid(rowIndex int) bool {
	return bitutil.BitIsSet(s.nullBitmapBytes, rowIndex)
}

func (s *Series) IsNull(rowIndex int) bool {
	return bitutil.BitIsNotSet(s.nullBitmapBytes, rowIndex)
}

func NewSeriesFromInterfaces(name string, typ Type, cells []interface{}) (Series, error) {
	var err error
	if typ == Unknown {
		if typ, err = seekType(cells); err != nil {
			return Series{}, err
		}
	}

	series := NewSeriesEmpty(name, len(cells), typ)
	for i, c := range cells {
		series.SetOrDrop(i, c)
	}

	return series, nil
}

func seekType(cells []interface{}) (Type, error) {
	for _, val := range cells {
		if val != nil {
			switch val.(type) {
			case float64, json.Number:
				return Float64, nil
			case int, int64:
				return Int64, nil
			case string:
				return String, nil
			case bool:
				return Boolean, nil
			}
		}
	}

	return Float64, nil
}
