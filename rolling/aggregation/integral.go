package aggregation

import (
	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
)

func IntegralTrapezoid(col string) rolling.ColumnAggregation {
	return rolling.NewColumnAggregation(col, true, bow.Float64,
		func(col int, w rolling.Window) (interface{}, error) {
			if w.Bow.NumRows() == 0 {
				return nil, nil
			}

			var sum float64
			var ok bool
			t0, v0, rowIndex := w.Bow.GetNextFloat64s(w.IntervalColumnIndex, col, 0)
			if rowIndex < 0 {
				return nil, nil
			}

			for rowIndex >= 0 {
				t1, v1, nextRowIndex := w.Bow.GetNextFloat64s(w.IntervalColumnIndex, col, rowIndex+1)
				if nextRowIndex < 0 {
					break
				}

				sum += (v0 + v1) / 2 * (t1 - t0)
				ok = true

				t0, v0, rowIndex = t1, v1, nextRowIndex
			}
			if !ok {
				return nil, nil
			}
			return sum, nil
		})
}

func IntegralStep(col string) rolling.ColumnAggregation {
	return rolling.NewColumnAggregation(col, false, bow.Float64,
		func(col int, w rolling.Window) (interface{}, error) {
			if w.Bow.NumRows() == 0 {
				return nil, nil
			}
			var sum float64
			var ok bool
			t0, v0, rowIndex := w.Bow.GetNextFloat64s(w.IntervalColumnIndex, col, 0)
			for rowIndex >= 0 {
				t1, v1, nextRowIndex := w.Bow.GetNextFloat64s(w.IntervalColumnIndex, col, rowIndex+1)
				if nextRowIndex < 0 {
					t1 = float64(w.End)
				}

				sum += v0 * (t1 - t0)
				ok = true

				if nextRowIndex < 0 {
					break
				}

				t0, v0, rowIndex = t1, v1, nextRowIndex
			}
			if !ok {
				return nil, nil
			}
			return sum, nil
		})
}
