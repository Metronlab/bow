package fill

import (
	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"
)

func StepPrevious(colName string) rolling.ColumnInterpolation {
	previousIndex := -1

	return rolling.NewColumnInterpolation(colName, []bow.Type{bow.Int64, bow.Float64, bow.Bool},
		func(inputCol int, outputType bow.Type, neededPos float64, w bow.Window, full bow.Bow) (interface{}, error) {
			if full.NumRows() == 0 {
				return nil, nil
			}

			index := previousIndex
			lastIndex := full.NumRows() - 1
			var pos float64
			for pos < neededPos {
				index++
				if index > lastIndex {
					break
				}
				pos, _ = full.GetFloat64(w.IntervalColumnIndex, index)
			}

			index--
			val := full.GetValue(inputCol, index)
			previousIndex = index

			return val, nil
		},
	)
}
