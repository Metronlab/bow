package interpolation

import (
	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
)

func StepPrevious(colName string) rolling.ColInterpolation {
	var prevVal interface{}
	return rolling.NewColumnInterpolation(colName, []bow.Type{bow.Int64, bow.Float64, bow.Bool, bow.String},
		func(colIndexToFill int, w rolling.Window, fullBow, prevRow bow.Bow) (interface{}, error) {
			// For the first window, add the previous row to interpolate correctly
			if w.FirstIndex == 0 && prevRow != nil {
				prevVal = prevRow.GetValue(colIndexToFill, prevRow.NumRows()-1)
			}

			var v interface{}
			_, v, _ = fullBow.GetPreviousValues(w.IntervalColumnIndex, colIndexToFill, w.FirstIndex-1)
			if v != nil {
				prevVal = v
			}

			return prevVal, nil
		},
	)
}
