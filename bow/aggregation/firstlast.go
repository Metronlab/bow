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

			value, row := w.Bow.GetNextFloat64(col, 0)
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

			value, row := w.Bow.GetPreviousFloat64(col, w.Bow.NumRows()-1)
			if row >= 0 {
				return value, nil
			}
			return nil, nil
		})
}
