package fill

import (
	"git.metronlab.com/backend_libraries/go-bow/bow"
)

func StepPrevious(colName string) bow.ColumnInterpolation {
	var previousIndex int = -1

	return bow.NewColumnInterpolation(colName, []bow.Type{bow.Int64, bow.Float64, bow.Bool},
		func(colIndex int, neededPos float64, w bow.Window, fullBow bow.Bow) (interface{}, error) {
			if fullBow.NumRows() == 0 {
				return nil, nil
			}

			index := previousIndex
			lastIndex := fullBow.NumRows() - 1
			var pos float64
			for pos < neededPos {
				index++
				if index > lastIndex {
					break
				}
				pos, _ = fullBow.GetFloat64(w.IntervalColumnIndex, index)
			}

			index--
			val := fullBow.GetValue(colIndex, index)

			previousIndex = index
			return val, nil
		},
	)
}
