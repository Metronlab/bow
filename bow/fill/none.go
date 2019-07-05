package fill

import "git.prod.metronlab.io/backend_libraries/go-bow/bow"
import "git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"

func None(colName string) rolling.ColumnInterpolation {
	return rolling.NewColumnInterpolation(colName, []bow.Type{bow.Int64, bow.Float64, bow.Bool},
		func(inputCol int, neededPos int64, window bow.Window, full bow.Bow) (interface{}, error) {
			return nil, nil
		},
	)
}
