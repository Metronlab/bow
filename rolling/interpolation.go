package rolling

import (
	"fmt"

	"github.com/metronlab/bow"
)

// ColInterpolation is used to interpolate a column.
type ColInterpolation struct {
	colName    string
	inputTypes []bow.Type
	fn         ColInterpolationFunc

	colIndex int
}

// ColInterpolationFunc is a function that take a column index, a Window, the full bow.Bow and the previous row, and provides a value at the start of the Window.
type ColInterpolationFunc func(colIndex int, window Window, fullBow, prevRow bow.Bow) (interface{}, error)

// NewColInterpolation returns a new ColInterpolation.
func NewColInterpolation(colName string, inputTypes []bow.Type, fn ColInterpolationFunc) ColInterpolation {
	return ColInterpolation{
		colName:    colName,
		inputTypes: inputTypes,
		fn:         fn,
	}
}

func (r *intervalRolling) Interpolate(interps ...ColInterpolation) Rolling {
	if r.err != nil {
		return r
	}

	rCopy := *r
	if len(interps) == 0 {
		return rCopy.setError(fmt.Errorf("at least one column interpolation is required"))
	}

	newIntervalCol := -1
	for i := range interps {
		isInterval, err := r.validateInterpolation(&interps[i], i)
		if err != nil {
			return rCopy.setError(fmt.Errorf("intervalRolling.validateInterpolation: %w", err))
		}
		if isInterval {
			newIntervalCol = i
		}
	}

	if newIntervalCol == -1 {
		return rCopy.setError(fmt.Errorf("must keep interval column '%s'", r.bow.ColumnName(r.intervalColIndex)))
	}

	b, err := rCopy.interpolateWindows(interps)
	if err != nil {
		return rCopy.setError(fmt.Errorf("intervalRolling.interpolateWindows: %w", err))
	}
	if b == nil {
		b = r.bow.NewEmptySlice()
	}

	newR, err := newIntervalRolling(b, newIntervalCol, rCopy.interval, rCopy.options)
	if err != nil {
		return rCopy.setError(fmt.Errorf("newIntervalRolling: %w", err))
	}

	return newR
}

func (r *intervalRolling) validateInterpolation(interp *ColInterpolation, newIndex int) (bool, error) {
	if interp.colName == "" {
		return false, fmt.Errorf("interpolation %d has no column name", newIndex)
	}

	var err error
	interp.colIndex, err = r.bow.ColumnIndex(interp.colName)
	if err != nil {
		return false, err
	}

	var typeOk bool
	colType := r.bow.ColumnType(interp.colIndex)
	for _, inputType := range interp.inputTypes {
		if colType == inputType {
			typeOk = true
			break
		}
	}
	if !typeOk {
		return false, fmt.Errorf("accepts types %v, got type %s",
			interp.inputTypes, colType)
	}

	return interp.colIndex == r.intervalColIndex, nil
}

func (r *intervalRolling) interpolateWindows(interps []ColInterpolation) (bow.Bow, error) {
	rCopy := *r

	bows := make([]bow.Bow, rCopy.numWindows)

	for rCopy.HasNext() {
		winIndex, w, err := rCopy.Next()
		if err != nil {
			return nil, err
		}

		bows[winIndex], err = rCopy.interpolateWindow(interps, w)
		if err != nil {
			return nil, err
		}
	}

	return bow.AppendBows(bows...)
}

func (r *intervalRolling) interpolateWindow(interps []ColInterpolation, window *Window) (bow.Bow, error) {
	var firstColValue int64 = -1
	if window.Bow.NumRows() > 0 {
		firstColVal, i := window.Bow.GetNextFloat64(r.intervalColIndex, 0)
		if i > -1 {
			firstColValue = int64(firstColVal)
		}
	}

	// has start: call interpolation anyway for those stateful
	if firstColValue == window.FirstValue {
		for _, interpolation := range interps {
			_, err := interpolation.fn(interpolation.colIndex, *window, r.bow, r.options.PrevRow)
			if err != nil {
				return nil, err
			}
		}

		return window.Bow, nil
	}

	// missing start
	series := make([]bow.Series, len(interps))
	for colIndex, interpolation := range interps {
		colType := window.Bow.ColumnType(interpolation.colIndex)

		interpolatedValue, err := interpolation.fn(interpolation.colIndex, *window, r.bow, r.options.PrevRow)
		if err != nil {
			return nil, err
		}

		buf := bow.NewBuffer(1, colType)
		buf.SetOrDrop(0, interpolatedValue)

		series[colIndex] = bow.NewSeriesFromBuffer(window.Bow.ColumnName(interpolation.colIndex), buf)
	}

	startBow, err := bow.NewBow(series...)
	if err != nil {
		return nil, err
	}

	return bow.AppendBows(startBow, window.Bow)
}
