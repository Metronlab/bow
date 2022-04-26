package bow

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/apache/arrow/go/v8/arrow"
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
		return val, err == nil
	case arrow.Timestamp:
		return int64(v), true
	default:
		return 0, false
	}
}

func ToFloat64(i interface{}) (float64, bool) {
	switch v := i.(type) {
	case float64:
		return v, true
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
	case bool:
		if v {
			return 1., true
		}
		return 0., true
	case string:
		val, err := strconv.ParseFloat(v, 64)
		return val, err == nil
	case arrow.Timestamp:
		return float64(v), true
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
	case json.Number:
		val, err := v.Float64()
		return val != 0., err != nil
	case int:
		return v != 0, true
	case int8:
		return v != 0, true
	case int16:
		return v != 0, true
	case int32:
		return v != 0, true
	case int64:
		return v != 0, true
	case float32:
		return v != 0, true
	case float64:
		return v != 0, true
	case arrow.Timestamp:
		return v != 0, true
	default:
		return false, false
	}
}

func ToString(i interface{}) (string, bool) {
	switch v := i.(type) {
	case bool:
		if v {
			return "true", true
		}
		return "false", true
	case string:
		return v, true
	case json.Number:
		return v.String(), true
	case int:
		return strconv.Itoa(v), true
	case int8:
		return strconv.Itoa(int(v)), true
	case int16:
		return strconv.Itoa(int(v)), true
	case int32:
		return strconv.Itoa(int(v)), true
	case int64:
		return strconv.Itoa(int(v)), true
	case float32:
		return fmt.Sprintf("%f", v), true
	case float64:
		return fmt.Sprintf("%f", v), true
	case arrow.Timestamp:
		return strconv.Itoa(int(v)), true
	default:
		return "", false
	}
}

func ToTimestamp(i interface{}) (arrow.Timestamp, bool) {
	switch v := i.(type) {
	case json.Number:
		val, err := v.Int64()
		return arrow.Timestamp(val), err == nil
	case int:
		return arrow.Timestamp(v), true
	case int8:
		return arrow.Timestamp(v), true
	case int16:
		return arrow.Timestamp(v), true
	case int32:
		return arrow.Timestamp(v), true
	case int64:
		return arrow.Timestamp(v), true
	case float32:
		return arrow.Timestamp(v), true
	case float64:
		return arrow.Timestamp(v), true
	case bool:
		if v {
			return 1, true
		}
		return 0, true
	case string:
		val, err := strconv.ParseInt(v, 10, 64)
		return arrow.Timestamp(val), err == nil
	case arrow.Timestamp:
		return v, true
	default:
		return 0, false
	}
}
