package fill

import "git.metronlab.com/backend_libraries/go-bow/bow"

func FillStepPrevious(colName string) bow.ColumnInterpolation {
	var currIndex int
	var previousVal interface{}
	var fn bow.ColumnInterpolationFunc
	fn = func(colIndex int, neededPos float64, w bow.Window) (interface{}, error) {
		var addedValue interface{}
		availablePos, _ := w.Bow.GetFloat64(w.IntervalColumnIndex, currIndex)
		if availablePos < float64(neededPos) {
			currIndex++
			return fn(colIndex, neededPos, w)
		}
		if float64(neededPos) == availablePos {
			availableVal := w.Bow.GetValue(colIndex, currIndex)
			if availableVal != nil {
				previousVal = availableVal
			}
		}
		if float64(neededPos) < availablePos {
			addedValue = previousVal
		}
		currIndex++
		return addedValue, nil
	}
	return bow.NewColumnInterpolation(
		colName,
		[]bow.Type{bow.Int64, bow.Float64, bow.Bool},
		fn,
	)
}
