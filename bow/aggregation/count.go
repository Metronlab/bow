package aggregation

import (
	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"
)

func Count(col string) rolling.ColumnAggregation {
	return rolling.NewColumnAggregation(col, false, bow.Int64,
		func(col int, w bow.Window) (interface{}, error) {
			if w.Bow.NumRows() == 0 {
				return nil, nil
			}

			var count int64
			for i := 0; i < w.Bow.NumRows(); i++ {
				v := w.Bow.GetValue(col, 0)
				if v != nil {
					count++
				}
			}
			return count, nil
		})
}
