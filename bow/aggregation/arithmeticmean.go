package aggregation

import (
	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
)

func ArithmeticMean() bow.ColumnAggregation {
	return bow.ColumnAggregation{
		Type: bow.Float64,
		Func: func(col int, w bow.Window) (interface{}, error) {
			t, err := w.Bow.GetType(col)
			if err != nil {
				return 0.0, err
			}

			if w.Bow.NumRows() == 0 {
				return nil, nil
			}

			var sum float64
			for i := int64(0); i < w.Bow.NumRows(); i++ {
				var value float64
				raw := w.Bow.GetValue(col, int(i))
				switch t {
				case bow.Int64:
					value = float64(raw.(int64))
				case bow.Float64:
					value = raw.(float64)
				}
				sum += value
			}
			return sum / float64(w.Bow.NumRows()), nil
		}}
}
