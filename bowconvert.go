package bow

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// ToInt64 attempts to convert `input` to int64.
// Return also a false boolean if the conversion failed.
func ToInt64(input interface{}) (output int64, ok bool) {
	switch value := input.(type) {
	case json.Number:
		output, err := value.Int64()
		return output, err == nil
	case int:
		return int64(value), true
	case int8:
		return int64(value), true
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return value, true
	case float32:
		return int64(value), true
	case float64:
		return int64(value), true
	case bool:
		if value {
			return 1, true
		}
		return 0, true
	case string:
		output, err := strconv.ParseInt(value, 10, 64)
		return output, err == nil
	}
	return
}

// ToFloat64 attempts to convert `input` to float64.
// Return also a false boolean if the conversion failed.
func ToFloat64(input interface{}) (output float64, ok bool) {
	switch input := input.(type) {
	case float64:
		return input, true
	case json.Number:
		output, err := input.Float64()
		return output, err == nil
	case int:
		return float64(input), true
	case int8:
		return float64(input), true
	case int16:
		return float64(input), true
	case int32:
		return float64(input), true
	case int64:
		return float64(input), true
	case float32:
		return float64(input), true
	case bool:
		if input {
			return 1., true
		}
		return 0., true
	case string:
		output, err := strconv.ParseFloat(input, 64)
		return output, err == nil
	}
	return
}

// ToBoolean attempts to convert `input` to bool.
// Return also a false boolean if the conversion failed.
// In case of numeric type, returns true if the value is non-zero.
func ToBoolean(input interface{}) (output bool, ok bool) {
	switch input := input.(type) {
	case bool:
		return input, true
	case string:
		output, err := strconv.ParseBool(input)
		return output, err == nil
	case json.Number:
		output, err := input.Float64()
		return output != 0., err != nil
	case int:
		return input != 0, true
	case int8:
		return input != 0, true
	case int16:
		return input != 0, true
	case int32:
		return input != 0, true
	case int64:
		return input != 0, true
	case float32:
		return input != 0., true
	case float64:
		return input != 0., true
	}
	return
}

// ToString attempts to convert `input` to string.
// Return also a false boolean if the conversion failed.
func ToString(input interface{}) (output string, ok bool) {
	switch input := input.(type) {
	case bool:
		if input {
			return "true", true
		}
		return "false", true
	case string:
		return input, true
	case json.Number:
		return input.String(), true
	case int:
		return strconv.Itoa(input), true
	case int8:
		return strconv.Itoa(int(input)), true
	case int16:
		return strconv.Itoa(int(input)), true
	case int32:
		return strconv.Itoa(int(input)), true
	case int64:
		return strconv.Itoa(int(input)), true
	case float32:
		return fmt.Sprintf("%f", input), true
	case float64:
		return fmt.Sprintf("%f", input), true
	}
	return
}
