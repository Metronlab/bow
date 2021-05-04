package fill

import (
	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
)

func Linear(colName string) rolling.ColInterpolation {
	return rolling.NewColInterpolation(colName, []bow.Type{bow.Int64, bow.Float64},
		func(inputCol int, w rolling.Window, fullBow, prevRow bow.Bow) (interface{}, error) {
			var rowIndex = w.FirstIndex
			var err error
			if prevRow != nil {
				fullBow, err = bow.AppendBows(prevRow, fullBow)
				if err != nil {
					return nil, err
				}
				rowIndex++
			}

			t0, v0, prevIndex := fullBow.GetPreviousFloat64s(w.IntervalColumnIndex, inputCol, rowIndex-1)
			if prevIndex == -1 {
				return nil, nil
			}
			t2, v2, nextIndex := fullBow.GetNextFloat64s(w.IntervalColumnIndex, inputCol, rowIndex)
			if nextIndex == -1 {
				return nil, nil
			}

			coef := (float64(w.Start) - t0) / (t2 - t0)
			return ((v2 - v0) * coef) + v0, nil
		},
	)
}
