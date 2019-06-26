package aggregation

import (
	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"
)

func First(col string) rolling.ColumnAggregation {
	return rolling.NewColumnAggregation(col, false, bow.Float64,
		func(col int, w bow.Window) (interface{}, error) {
			if w.Bow.NumRows() == 0 {
				return nil, nil
			}

			// TODO: use a getNextVal
			value, row := getNextFloat64(w.Bow, col, 0)
			if row >= 0 {
				return value, nil
			}
			return nil, nil
		})
}

func Last(col string) rolling.ColumnAggregation {
	return rolling.NewColumnAggregation(col, false, bow.Float64,
		func(col int, w bow.Window) (interface{}, error) {
			if w.Bow.NumRows() == 0 {
				return nil, nil
			}

			// TODO: use a getPreviousVal
			value, row := getNextFloat64(w.Bow, col, 0)
			if row >= 0 {
				return value, nil
			}
			return nil, nil
		})
}
