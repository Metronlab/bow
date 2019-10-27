package aggregation

import (
	"github.com/Metronlab/bow/bow"
	"github.com/Metronlab/bow/bow/rolling"
)

func ArithmeticMean(col string) rolling.ColumnAggregation {
	return rolling.NewColumnAggregation(col, false, bow.Float64,
		func(col int, w bow.Window) (interface{}, error) {
			if w.Bow.NumRows() == 0 {
				return nil, nil
			}

			var sum float64
			var count int
			for i := 0; i < w.Bow.NumRows(); i++ {
				value, ok := w.Bow.GetFloat64(col, i)
				if !ok {
					continue
				}
				sum += value
				count++
			}
			if count == 0 {
				return nil, nil
			}
			return sum / float64(count), nil
		})
}
