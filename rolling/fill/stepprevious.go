package fill

import (
	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
)

func StepPrevious(colName string) rolling.ColInterpolation {
	return rolling.NewColInterpolation(colName, []bow.Type{bow.Int64, bow.Float64, bow.Bool, bow.String},
		func(inputCol int, w rolling.Window, fullBow, prevRow bow.Bow) (interface{}, error) {
			var rowIndexToInterpolate = w.FirstIndex

			/*
				var err error
				if prevRow != nil && rowIndexToInterpolate == 0 {
					fullBow, err = bow.AppendBows(prevRow, fullBow)
					if err != nil {
						return nil, err
					}
					rowIndexToInterpolate = 1
				}
			*/

			_, v, _ := fullBow.GetPreviousValues(w.IntervalColumnIndex, inputCol, rowIndexToInterpolate-1)
			return v, nil
		},
	)
}
