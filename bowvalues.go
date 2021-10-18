package bow

import "github.com/apache/arrow/go/arrow/array"

func Int64Values(arr *array.Int64) []int64 {
	return arr.Int64Values()
}

func Float64Values(arr *array.Float64) []float64 {
	return arr.Float64Values()
}

func BooleanValues(arr *array.Boolean) []bool {
	res := make([]bool, arr.Len())
	for i := range res {
		res[i] = arr.Value(i)
	}
	return res
}

func StringValues(arr *array.String) []string {
	res := make([]string, arr.Len())
	for i := range res {
		res[i] = arr.Value(i)
	}
	return res
}
