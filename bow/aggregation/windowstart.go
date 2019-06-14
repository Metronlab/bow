package aggregation

import "git.prod.metronlab.io/backend_libraries/go-bow/bow"

func WindowStart(col string) bow.ColumnAggregation {
	return bow.NewColumnAggregation(col, bow.Int64,
		func(col int, w bow.Window) (interface{}, error) {
			return w.Start, nil
		})
}
