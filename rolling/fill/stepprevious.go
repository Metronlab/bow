package fill

import (
	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
)

func StepPrevious(colName string) rolling.ColumnInterpolation {
	return rolling.NewColumnInterpolation(colName, []bow.Type{bow.Int64, bow.Float64, bow.Bool, bow.String},
		func(inputCol int, w rolling.Window, full bow.Bow) (interface{}, error) {
			_, v, _ := full.GetPreviousValues(w.IntervalColumnIndex, inputCol, w.FirstIndex-1)
			return v, nil
		},
	)
}