package bow

import (
	"encoding/json"
	"fmt"

	"github.com/apache/arrow/go/arrow/bitutil"
)

type Series struct {
	Name            string
	Data            interface{}
	nullBitmapBytes []byte
}

func (s *Series) GetFloat64(rowIndex int) (float64, bool) {
	if rowIndex < 0 || rowIndex >= s.Len() {
		return 0., false
	}

	switch s.DataType() {
	case Float64:
		return s.Data.([]float64)[rowIndex], s.IsValid(rowIndex)
	case Int64:
		return float64(s.Data.([]int64)[rowIndex]), s.IsValid(rowIndex)
	case Boolean:
		booleanValue := s.Data.([]bool)[rowIndex]
		if booleanValue {
			return 1., s.IsValid(rowIndex)
		}
		return 0., s.IsValid(rowIndex)
	case String:
		if s.IsValid(rowIndex) {
			return ToFloat64(s.Data.([]string)[rowIndex])
		}
		return 0., false
	default:
		panic(fmt.Sprintf("unsupported type '%s'", s.DataType()))
	}
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
