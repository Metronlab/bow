package aggregation

import (
	"fmt"
	"git.metronlab.com/backend_libraries/go-bow/bow"
)

func ArithmeticMean(col string) bow.ColumnAggregation {
	return bow.NewColumnAggregation(col, bow.Float64,
		func(col int, w bow.Window) (interface{}, error) {
			if w.Bow.NumRows() == 0 {
				return nil, nil
			}

			var sum float64
			for i := int64(0); i < w.Bow.NumRows(); i++ {
				value, ok := w.Bow.GetFloat64(col, int(i))
				if !ok {
					continue
				}
				sum += value
			}
			return sum / float64(w.Bow.NumRows()), nil
		})
}

func WeightedAverage(col string) bow.ColumnAggregation {
	return bow.NewColumnAggregation(col, bow.Float64,
		func(col int, w bow.Window) (interface{}, error) {
			if w.Bow.NumRows() == 0 {
				return nil, nil
			}

			windowsWide := float64(w.End - w.Start)

			var sum float64
			t0, v0, rowIndex := getNextFloat64s(w.Bow, w.IntervalColumnIndex, col, 0)
			if rowIndex < 0 {
				return nil, nil
			}

			for rowIndex >= 0 {
				t1, v1, nextRowIndex := getNextFloat64s(w.Bow, w.IntervalColumnIndex, col, rowIndex+1)
				if nextRowIndex < 0 {
					t1 = float64(w.End)
				}

				sum += v0 * (t1 - t0)

				if nextRowIndex < 0 {
					break
				}

				t0, v0, rowIndex = t1, v1, nextRowIndex
			}
			return sum / windowsWide, nil
		})
}

func getNextFloat64s(b bow.Bow, col1, col2, row int) (float64, float64, int) {
	if row >= int(b.NumRows()) {
		return 0., 0., -1
	}

	var v1, v2 float64
	var row2 int
	for v1, row = getNextFloat64(b, col1, row); row >= 0 && row < int(b.NumRows()); {
		if v2, row2 = getNextFloat64(b, col2, row); row == row2 {
			return v1, v2, row
		}

		row++
		if row >= int(b.NumRows()) {
			return 0., 0., -1
		}
	}

	return 0., 0., -1
}

func getNextFloat64(b bow.Bow, col, row int) (float64, int) {
	if row >= int(b.NumRows()) {
		return 0., -1
	}

	var value float64
	var ok bool
	for value, ok = b.GetFloat64(col, row); row < int(b.NumRows()); {
		if ok {
			return value, row
		}
		row++
	}
	return 0., -1
}
