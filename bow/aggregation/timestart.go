package aggregation

import "git.prod.metronlab.io/backend_libraries/go-bow/bow"

func TimeStart() bow.ColumnAggregation {
	return bow.ColumnAggregation{
		Type: bow.Int64,
		Func: func(col int, w bow.Window) (interface{}, error) {
			return w.Start, nil
		}}
}
