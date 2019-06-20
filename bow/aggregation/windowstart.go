package aggregation

import "git.metronlab.com/backend_libraries/go-bow/bow"

func WindowStart(col string) bow.ColumnAggregation {
	return bow.NewColumnAggregation(col, bow.Float64,
		func(col int, w bow.Window) (interface{}, error) {
			return w.Start, nil
		})
}
