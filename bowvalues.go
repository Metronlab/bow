package bow

import (
	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
)

func int64Values(arr *array.Int64) []int64 {
	return arr.Int64Values()
}

func float64Values(arr *array.Float64) []float64 {
	return arr.Float64Values()
}

func booleanValues(arr *array.Boolean) []bool {
	var res = make([]bool, arr.Len())
	for i := range res {
		res[i] = arr.Value(i)
	}
	return res
}

func stringValues(arr *array.String) []string {
	var res = make([]string, arr.Len())
	for i := range res {
		res[i] = arr.Value(i)
	}
	return res
}

func timestampValues(arr *array.Timestamp) []arrow.Timestamp {
	var res = make([]arrow.Timestamp, arr.Len())
	for i := range res {
		res[i] = arr.Value(i)
	}
	return res
}
