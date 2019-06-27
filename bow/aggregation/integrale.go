package aggregation

import (
	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"
)

func IntegralTrapezoid(col string) rolling.ColumnAggregation {
	return rolling.NewColumnAggregation(col, true, bow.Float64,
		func(col int, w bow.Window) (interface{}, error) {
			if w.Bow.NumRows() == 0 {
				return nil, nil
			}

			var sum float64
			var ok bool
			t0, v0, rowIndex := getNextFloat64s(w.Bow, w.IntervalColumnIndex, col, 0)
			if rowIndex < 0 {
				return nil, nil
			}

			for rowIndex >= 0 {
				t1, v1, nextRowIndex := getNextFloat64s(w.Bow, w.IntervalColumnIndex, col, rowIndex+1)
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
		func(col int, w bow.Window) (interface{}, error) {
			if w.Bow.NumRows() == 0 {
				return nil, nil
			}
			var sum float64
			var ok bool
			t0, v0, rowIndex := getNextFloat64s(w.Bow, w.IntervalColumnIndex, col, 0)
			for rowIndex >= 0 {
				t1, v1, nextRowIndex := getNextFloat64s(w.Bow, w.IntervalColumnIndex, col, rowIndex+1)
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

func getNextFloat64s(b bow.Bow, col1, col2, row int) (float64, float64, int) {
	if row < 0 || row >= int(b.NumRows()) {
		return 0., 0., -1
	}

	var v1, v2 float64
	var row2 int
	v1, row = getNextFloat64(b, col1, row)
	for row >= 0 && row < int(b.NumRows()) {
		v2, row2 = getNextFloat64(b, col2, row)
		if row == row2 {
			return v1, v2, row
		}

		row++
		v1, row = getNextFloat64(b, col1, row)
	}

	return 0., 0., -1
}

func getNextFloat64(b bow.Bow, col, row int) (float64, int) {
	if row < 0 || row >= int(b.NumRows()) {
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

//
//func getPreviousFloat64s(b bow.Bow, col1, col2, row int) (float64, float64, int) {
//	if row < 0 || row >= int(b.NumRows()) {
//		return 0., 0., -1
//	}
//
//	var v1, v2 float64
//	var row2 int
//	for v1, row = getPreviousFloat64(b, col1, row); row >= 0 && row < int(b.NumRows()); {
//		if v2, row2 = getPreviousFloat64(b, col2, row); row == row2 {
//			return v1, v2, row
//		}
//
//		row--
//	}
//
//	return 0., 0., -1
//}
//
//func getPreviousFloat64(b bow.Bow, col, row int) (float64, int) {
//	if row < 0 || row >= int(b.NumRows()) {
//		return 0., -1
//	}
//
//	var value float64
//	var ok bool
//	for value, ok = b.GetFloat64(col, row); row >= 0; {
//		if ok {
//			return value, row
//		}
//		row--
//	}
//	return 0., -1
//}
