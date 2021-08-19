package interpolation

import (
	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
)

func Linear(colName string) rolling.ColInterpolation {
	var prevT0, prevV0 float64
	var prevValid bool
	return rolling.NewColInterpolation(colName, []bow.Type{bow.Int64, bow.Float64},
		func(colIndexToFill int, w rolling.Window, fullBow, prevRow bow.Bow) (interface{}, error) {
			var prevValidT0, prevValidV0 bool
			if w.FirstIndex == 0 && prevRow != nil {
				prevT0, prevValidT0 = prevRow.GetFloat64(w.IntervalColIndex, prevRow.NumRows()-1)
				prevV0, prevValidV0 = prevRow.GetFloat64(colIndexToFill, prevRow.NumRows()-1)
				prevValid = prevValidT0 && prevValidV0
			}

			t0, v0, prevIndex := fullBow.GetPreviousFloat64s(w.IntervalColIndex, colIndexToFill, w.FirstIndex-1)
			if prevIndex == -1 {
				if !prevValid {
					return nil, nil
				}
				t0 = prevT0
				v0 = prevV0
			}

			t2, v2, nextIndex := fullBow.GetNextFloat64s(w.IntervalColIndex, colIndexToFill, w.FirstIndex)
			if nextIndex == -1 {
				return nil, nil
			}

			coef := (float64(w.Start) - t0) / (t2 - t0)
			return ((v2 - v0) * coef) + v0, nil
		},
	)
}
