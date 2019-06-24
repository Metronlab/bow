package aggregation

import (
	"git.metronlab.com/backend_libraries/go-bow/bow"
	"git.metronlab.com/backend_libraries/go-bow/bow/rolling"
)

func Erring(col string, err error) rolling.ColumnAggregation {
	return rolling.NewColumnAggregation(col, bow.Float64,
		func(col int, w bow.Window) (interface{}, error) {
			return nil, err
		})
}
