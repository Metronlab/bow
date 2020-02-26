package fill

import (
	"github.com/metronlab/bow/bow"
	"github.com/metronlab/bow/bow/rolling"
)

func Linear(colName string) rolling.ColumnInterpolation {
	return rolling.NewColumnInterpolation(colName, []bow.Type{bow.Int64, bow.Float64},
		func(inputCol int, w rolling.Window, full bow.Bow) (interface{}, error) {
			t0, v0, prevIndex := full.GetPreviousFloat64s(w.IntervalColumnIndex, inputCol, w.FirstIndex-1)
			if prevIndex == -1 {
				return nil, nil
			}
			t2, v2, nextIndex := full.GetNextFloat64s(w.IntervalColumnIndex, inputCol, w.FirstIndex)
			if nextIndex == -1 {
				return nil, nil
			}

			coef := (float64(w.Start) - t0) / (t2 - t0)
			return ((v2 - v0) * coef) + v0, nil
		},
	)
}
