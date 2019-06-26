package bow

import (
	"encoding/json"
	"strconv"
)

func ToInt64(i interface{}) (int64, bool) {
	switch v := i.(type) {
	case json.Number:
		val, err := v.Int64()
		return val, err == nil
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case float32:
		return int64(v), true
	case float64:
		return int64(v), true
	case bool:
		if v {
			return 1, true
		}
		return 0, true
	case string:
		val, err := strconv.ParseInt(v, 10, 64)
		return int64(val), err == nil
	default:
		return 0, false
	}
}

func ToFloat64(i interface{}) (float64, bool) {
	switch v := i.(type) {
	case json.Number:
		val, err := v.Float64()
		return val, err == nil
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return float64(v), true
	case bool:
		if v {
			return 1, true
		}
		return 0, true
	case string:
		val, err := strconv.ParseFloat(v, 64)
		return val, err == nil
	default:
		return 0, false
	}
}

func ToBool(i interface{}) (bool, bool) {
	switch v := i.(type) {
	case bool:
		return v, true
	case string:
		val, err := strconv.ParseBool(v)
		return val, err == nil
	default:
		return false, false
	}
}
