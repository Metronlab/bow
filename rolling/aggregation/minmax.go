package aggregation

import (
	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
)

func Min(col string) rolling.ColAggregation {
	return rolling.NewColumnAggregation(col, false, bow.Float64,
		func(col int, w rolling.Window) (interface{}, error) {
			if w.Bow.IsEmpty() {
				return nil, nil
			}

			var min interface{}
			for i := 0; i < w.Bow.NumRows(); i++ {
				value, ok := w.Bow.GetFloat64(col, i)
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

func Max(col string) rolling.ColAggregation {
	return rolling.NewColumnAggregation(col, false, bow.Float64,
		func(col int, w rolling.Window) (interface{}, error) {
			if w.Bow.IsEmpty() {
				return nil, nil
			}

			var min interface{}
			for i := 0; i < w.Bow.NumRows(); i++ {
				value, ok := w.Bow.GetFloat64(col, i)
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
