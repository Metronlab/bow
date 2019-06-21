package fill

import "git.metronlab.com/backend_libraries/go-bow/bow"

func IntervalPosition(colName string) bow.ColumnInterpolation {
	return bow.NewColumnInterpolation(colName, []bow.Type{bow.Int64, bow.Float64},
		func(colIndex int, neededPos float64, w bow.Window, fullBow bow.Bow) (interface{}, error) {
			return neededPos, nil
		},
	)
}
