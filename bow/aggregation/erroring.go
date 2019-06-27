package aggregation

import (
	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"
)

func Erring(col string, err error) rolling.ColumnAggregation {
	return rolling.NewColumnAggregation(col, false, bow.Float64,
		func(col int, w bow.Window) (interface{}, error) {
			return nil, err
		})
}