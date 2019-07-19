package fill

import (
	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"
)

func StepPrevious(colName string) rolling.ColumnInterpolation {
	index := -1
	var lastVal interface{}

	return rolling.NewColumnInterpolation(colName, []bow.Type{bow.Int64, bow.Float64, bow.Bool},
		func(inputCol int, neededPos int64, w bow.Window, full bow.Bow) (interface{}, error) {
			if full.NumRows() == 0 {
				return nil, nil
			}

			for {
				nextPos, nextIndex := full.GetNextFloat64(w.IntervalColumnIndex, index+1)
				if index > -1 {
					v := full.GetValue(inputCol, index)
					if v != nil {
						lastVal = v
					}
				}
				if nextIndex == -1 || int64(nextPos) >= neededPos {
					return lastVal, nil
				}
				index = nextIndex
			}
		},
	)
}
