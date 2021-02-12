package aggregation

import (
	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
)

func First(col string) rolling.ColumnAggregation {
	return rolling.NewColumnAggregation(col, false, bow.InputDependent,
		func(col int, w rolling.Window) (interface{}, error) {
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
		func(col int, w rolling.Window) (interface{}, error) {
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
