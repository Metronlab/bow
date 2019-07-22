package fill

import (
	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"
)

func Linear(colName string) rolling.ColumnInterpolation {
	previousIndex := -1

	return rolling.NewColumnInterpolation(colName, []bow.Type{bow.Int64, bow.Float64, bow.Bool},
		func(colIndex int, neededPos int64, w bow.Window, fullBow bow.Bow) (interface{}, error) {
			if fullBow.NumRows() == 0 {
				return nil, nil
			}

			index := previousIndex
			lastIndex := fullBow.NumRows() - 1
			var nextPos int64
			for nextPos < neededPos {
				index++
				if index > lastIndex {
					break
				}
				nextPos, _ = fullBow.GetInt64(w.IntervalColumnIndex, index)
			}

			if index > lastIndex {
				return nil, nil
			}

			prevPosFloat, prevVal, prevIndex := fullBow.GetPreviousFloat64s(w.IntervalColumnIndex, colIndex, index-1)
			if prevIndex < 0 {
				return nil, nil
			}

			nextPosFloat, nextVal, nextIndex := fullBow.GetNextFloat64s(w.IntervalColumnIndex, colIndex, index)
			if nextIndex < 0 {
				return nil, nil
			}

			coefficient := (float64(neededPos) - prevPosFloat) / (nextPosFloat - prevPosFloat)

			return ((nextVal - prevVal) * coefficient) + prevVal, nil
		},
	)
}