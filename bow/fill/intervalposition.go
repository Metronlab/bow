package fill

import "git.prod.metronlab.io/backend_libraries/go-bow/bow"
import "git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"

func IntervalPosition(colName string) rolling.ColumnInterpolation {
	return rolling.NewColumnInterpolation(colName, []bow.Type{bow.Int64, bow.Float64},
		func(colIndex int, neededPos float64, w bow.Window, fullBow bow.Bow) (interface{}, error) {
			return neededPos, nil
		},
	)
}
