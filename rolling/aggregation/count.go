package aggregation

import (
	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
)

func Count(col string) rolling.ColAggregation {
	return rolling.NewColAggregation(col, false, bow.Int64,
		func(col int, w rolling.Window) (interface{}, error) {
			var count int64
			for i := 0; i < w.Bow.NumRows(); i++ {
				v := w.Bow.GetValue(col, i)
				if v != nil {
					count++
				}
			}
			return count, nil
		})
}
