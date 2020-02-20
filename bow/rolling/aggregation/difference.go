package aggregation

import (
	"github.com/metronlab/bow/bow"
	"github.com/metronlab/bow/bow/rolling"
)

func Difference(col string) rolling.ColumnAggregation {
	first := First(col).Func()
	last := Last(col).Func()

	return rolling.NewColumnAggregation(col, true, bow.Float64,
		func(col int, w bow.Window) (interface{}, error) {
			if w.Bow.NumRows() == 0 {
				return nil, nil
			}

			f, err := first(col, w)
			if f == nil || err != nil {
				return nil, err
			}
			l, err := last(col, w)
			if l == nil || err != nil {
				return nil, err
			}

			floatedF := bow.Float64.Convert(f)
			floatedL := bow.Float64.Convert(l)

			if floatedF == nil || floatedL == nil {
				return nil, nil
			}

			return floatedL.(float64) - floatedF.(float64), nil
		})
}
