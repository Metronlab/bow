package fill

import "git.prod.metronlab.io/backend_libraries/go-bow/bow"
import "git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"

func WindowStart(colName string) rolling.ColumnInterpolation {
	return rolling.NewColumnInterpolation(colName, []bow.Type{bow.Int64},
		func(inputCol int, w bow.Window, full bow.Bow) (interface{}, error) {
			return w.Start, nil
		},
	)
}
