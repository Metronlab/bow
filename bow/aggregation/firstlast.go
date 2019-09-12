package aggregation

import (
	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"
)

func First(col string) rolling.ColumnAggregation {
	return rolling.NewColumnAggregation(col, false, bow.InputDependent,
		func(col int, w bow.Window) (interface{}, error) {
			if w.Bow.NumRows() == 0 {
				return nil, nil
			}

			value, irow := w.Bow.GetNextValue(col, 0)
			if irow == -1 {
				return nil, nil
			}
			return value, nil
		})
}

func Last(col string) rolling.ColumnAggregation {
	return rolling.NewColumnAggregation(col, false, bow.InputDependent,
		func(col int, w bow.Window) (interface{}, error) {
			if w.Bow.NumRows() == 0 {
				return nil, nil
			}

			value, irow := w.Bow.GetPreviousValue(col, w.Bow.NumRows()-1)
			if irow == -1 {
				return nil, nil
			}
			return value, nil
		})
}
