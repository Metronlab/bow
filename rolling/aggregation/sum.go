package aggregation

import (
	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
)

func Sum(col string) rolling.ColAggregation {
	return rolling.NewColAggregation(col, false, bow.Float64,
		func(col int, w rolling.Window) (interface{}, error) {
			if w.Bow.NumRows() == 0 {
				return 0., nil
			}

			var sum float64
			for i := 0; i < w.Bow.NumRows(); i++ {
				value, ok := w.Bow.GetFloat64(col, i)
				if !ok {
					continue
				}
				sum += value
			}
			return sum, nil
		})
}
