package fill

import (
	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"
)

func Linear(colName string) rolling.ColumnInterpolation {
	previousIndex := -1

	return rolling.NewColumnInterpolation(colName, []bow.Type{bow.Int64, bow.Float64, bow.Bool},
		func(colIndex int, neededPos float64, w bow.Window, fullBow bow.Bow) (interface{}, error) {
			if fullBow.NumRows() == 0 {
				return nil, nil
			}

			index := previousIndex
			lastIndex := fullBow.NumRows() - 1
			var nextPos float64
			for nextPos < neededPos {
				index++
				if index > lastIndex {
					break
				}
				nextPos, _ = fullBow.GetFloat64(w.IntervalColumnIndex, index)
			}

			// from there we have the index where the value should be filled
			// we need to get the value before, and after to calculate the linear interpolation
			if index > lastIndex {
				return nil, nil
			}

			prevPos, prevVal, prevIndex := fullBow.GetPreviousFloat64s(w.IntervalColumnIndex, colIndex, index - 1)
			if prevIndex < 0 {
				return nil, nil
			}

			nextPos, nextVal, nextIndex := fullBow.GetNextFloat64s(w.IntervalColumnIndex, colIndex, index)
			if nextIndex < 0 {
				return nil, nil
			}

			percentage := (neededPos - prevPos) / (nextPos - prevPos)

			return ((nextVal - prevVal) * percentage) + prevVal, nil
		},
	)
}

