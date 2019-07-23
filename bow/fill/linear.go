package fill

import (
	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"
)

func Linear(colName string) rolling.ColumnInterpolation {
	lastIndex := -1

	updateLastIndex := func(w bow.Window) {
		if w.FirstIndex != -1 {
			lastIndex = w.FirstIndex + w.Bow.NumRows() - 1
		}
	}

	return rolling.NewColumnInterpolation(colName, []bow.Type{bow.Int64, bow.Float64, bow.Bool},
		func(inputCol int, w bow.Window, full bow.Bow) (interface{}, error) {
			prevPos, prevVal, prevIndex := full.GetPreviousFloat64s(w.IntervalColumnIndex, inputCol, lastIndex)
			if prevIndex == -1 {
				updateLastIndex(w)
				return nil, nil
			}
			nextPos, nextVal, nextIndex := full.GetNextFloat64s(w.IntervalColumnIndex, inputCol, lastIndex+1)
			if nextIndex == -1 {
				updateLastIndex(w)
				return nil, nil
			}
			updateLastIndex(w)

			coefficient := (float64(w.Start) - prevPos) / (nextPos - prevPos)
			val := ((nextVal - prevVal) * coefficient) + prevVal
			return val, nil
		},
	)
}
