package fill

import (
	"github.com/Metronlab/bow/bow"
	"github.com/Metronlab/bow/bow/rolling"
)

func StepPrevious(colName string) rolling.ColumnInterpolation {
	var lastVal interface{}

	return rolling.NewColumnInterpolation(colName, []bow.Type{bow.Int64, bow.Float64, bow.Bool},
		func(inputCol int, w bow.Window, full bow.Bow) (interface{}, error) {
			if w.FirstIndex == -1 {
				return lastVal, nil
			}
			_, v, _ := full.GetPreviousValues(w.IntervalColumnIndex, inputCol, w.FirstIndex-1)
			lastVal = w.Bow.GetValue(inputCol, w.Bow.NumRows()-1)
			return v, nil
		},
	)
}
