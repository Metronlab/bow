package fill

import (
	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
)

func StepPrevious(colName string) rolling.ColumnInterpolation {
	return rolling.NewColumnInterpolation(colName, []bow.Type{bow.Int64, bow.Float64, bow.Bool, bow.String},
		func(colIndexToFill int, w rolling.Window, fullBow, prevRow bow.Bow) (interface{}, error) {
			var rowIndexToFill = w.FirstIndex

			var err error
			prevRow, err = rolling.ValidatePrevRow(fullBow, prevRow)
			if err != nil {
				panic(err)
			}

			// For the first window, add the previous row to interpolate correctly
			if prevRow != nil && rowIndexToFill == 0 {
				fullBow, err = bow.AppendBows(prevRow, fullBow)
				if err != nil {
					return nil, err
				}
				rowIndexToFill = 1
			}

			_, v, _ := fullBow.GetPreviousValues(w.IntervalColumnIndex, colIndexToFill, rowIndexToFill-1)
			return v, nil
		},
	)
}
