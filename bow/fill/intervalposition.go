package fill

import "git.prod.metronlab.io/backend_libraries/go-bow/bow"
import "git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"

func IntervalPosition(colName string) rolling.ColumnInterpolation {
	return rolling.NewColumnInterpolation(colName, []bow.Type{bow.Int64, bow.Float64},
		func(inputCol int, outputType bow.Type, neededPos float64, window bow.Window, fullBow bow.Bow) (interface{}, error) {
			return outputType.Cast(neededPos)
		},
	)
}
