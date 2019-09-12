package aggregation

import (
	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"
)

func Min(col string) rolling.ColumnAggregation {
	return rolling.NewColumnAggregation(col, false, bow.Float64,
		func(col int, w bow.Window) (interface{}, error) {
			if w.Bow.NumRows() == 0 {
				return nil, nil
			}

			var min interface{}
			for i := 0; i < w.Bow.NumRows(); i++ {
				value, ok := w.Bow.GetFloat64(col, int(i))
				if !ok {
					continue
				}
				if min != nil {
					if value < min.(float64) {
						min = value
					}
					continue
				}
				min = value
			}
			return min, nil
		})
}

func Max(col string) rolling.ColumnAggregation {
	return rolling.NewColumnAggregation(col, false, bow.Float64,
		func(col int, w bow.Window) (interface{}, error) {
			if w.Bow.NumRows() == 0 {
				return nil, nil
			}

			var min interface{}
			for i := 0; i < w.Bow.NumRows(); i++ {
				value, ok := w.Bow.GetFloat64(col, int(i))
				if !ok {
					continue
				}
				if min != nil {
					if value > min.(float64) {
						min = value
					}
					continue
				}
				min = value
			}
			return min, nil
		})
}
