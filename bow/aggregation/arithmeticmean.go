package aggregation

import (
	"git.metronlab.com/backend_libraries/go-bow/bow"
)

func ArithmeticMean(col string) bow.ColumnAggregation {
	return bow.NewColumnAggregation(col, bow.Float64,
		func(col int, w bow.Window) (interface{}, error) {
			if w.Bow.NumRows() == 0 {
				return nil, nil
			}

			var sum float64
			for i := 0; i < w.Bow.NumRows(); i++ {
				value, ok := w.Bow.GetFloat64(col, int(i))
				if !ok {
					continue
				}
				sum += value
			}
			return sum / float64(w.Bow.NumRows()), nil
		})
}
