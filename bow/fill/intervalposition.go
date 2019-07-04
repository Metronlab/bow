package fill

import "git.prod.metronlab.io/backend_libraries/go-bow/bow"
import "git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"

func IntervalPosition(colName string) rolling.ColumnInterpolation {
	return rolling.NewColumnInterpolation(colName, []bow.Type{bow.Int64},
		func(inputCol int, neededPos int64, window bow.Window, full bow.Bow) (interface{}, error) {
			return neededPos, nil
		},
	)
}
