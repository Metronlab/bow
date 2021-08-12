package rolling

import (
	"fmt"

	"github.com/metronlab/bow"
)

// ColInterpolationFunc provides a value at the start of `window`.
type ColInterpolationFunc func(inputCol int, window Window, fullBow, prevRow bow.Bow) (interface{}, error)

func NewColInterpolation(colName string, inputTypes []bow.Type, fn ColInterpolationFunc) ColInterpolation {
	return ColInterpolation{
		colName:    colName,
		inputTypes: inputTypes,
		fn:         fn,
	}
}

type ColInterpolation struct {
	colName    string
	inputTypes []bow.Type
	fn         ColInterpolationFunc

	colIndex int
}

// Interpolate each window by interpolating its start if missing
func (it *intervalRollingIter) Interpolate(interps ...ColInterpolation) Rolling {
	if it.err != nil {
		return it
	}

	itCopy := *it
	newIntervalCol, interps, err := itCopy.indexedInterpolations(interps)
	if err != nil {
		return itCopy.setError(fmt.Errorf("interpolate: %w", err))
	}

	b, err := itCopy.interpolateWindows(interps)
	if err != nil {
		return itCopy.setError(fmt.Errorf("interpolate: %w", err))
	}
	if b == nil {
		b = it.bow.NewEmpty()
	}

	newIt, err := IntervalRollingForIndex(b, newIntervalCol, itCopy.interval, itCopy.options)
	if err != nil {
		return itCopy.setError(fmt.Errorf("interpolate: %w", err))
	}

	return newIt
}

func (it *intervalRollingIter) indexedInterpolations(interps []ColInterpolation) (int, []ColInterpolation, error) {
	if len(interps) == 0 {
		return -1, nil, fmt.Errorf("at least one column interpolation is required")
	}

	newIntervalCol := -1
	for i := range interps {
		isInterval, err := it.validateInterpolation(&interps[i], i)
		if err != nil {
			return -1, nil, err
		}
		if isInterval {
			newIntervalCol = i
		}
	}

	if newIntervalCol == -1 {
		return -1, nil, fmt.Errorf("must keep interval column '%s'", it.bow.GetColName(it.colIndex))
	}

	return newIntervalCol, interps, nil
}

func (it *intervalRollingIter) validateInterpolation(interp *ColInterpolation, newIndex int) (bool, error) {
	if interp.colName == "" {
		return false, fmt.Errorf("interpolation %d has no column name", newIndex)
	}
	readIndex, err := it.bow.GetColIndex(interp.colName)
	if err != nil {
		return false, err
	}
	interp.colIndex = readIndex

	var typeOk bool
	typ := it.bow.GetColType(interp.colIndex)
	for _, inputTyp := range interp.inputTypes {
		if typ == inputTyp {
			typeOk = true
			break
		}
	}
	if !typeOk {
		return false, fmt.Errorf("accepts types %v, got type %s",
			interp.inputTypes, typ.String())
	}

	return readIndex == it.colIndex, nil
}

func (it *intervalRollingIter) interpolateWindows(interps []ColInterpolation) (bow.Bow, error) {
	it2 := *it

	bows := make([]bow.Bow, it2.numWindows)

	for it2.HasNext() {
		winIndex, w, err := it2.Next()
		if err != nil {
			return nil, err
		}

		bows[winIndex], err = it2.interpolateWindow(interps, w)
		if err != nil {
			return nil, err
		}
	}

	return bow.AppendBows(bows...)
}

func (it *intervalRollingIter) interpolateWindow(interps []ColInterpolation, w *Window) (bow.Bow, error) {
	var firstColValue int64 = -1
	if w.Bow.NumRows() > 0 {
		firstColVal, i := w.Bow.GetNextFloat64(it.colIndex, 0)
		if i > -1 {
			firstColValue = int64(firstColVal)
		}
	}

	// has start: call interpolation anyway for those stateful
	if firstColValue == w.Start {
		for _, interpolation := range interps {
			_, err := interpolation.fn(interpolation.colIndex, *w, it.bow, it.options.PrevRow)
			if err != nil {
				return nil, err
			}
		}

		return w.Bow, nil
	}

	// missing start
	seriesSlice := make([]bow.Series, len(interps))
	for colIndex, interpolation := range interps {
		colType := w.Bow.GetColType(interpolation.colIndex)

		interpolatedValue, err := interpolation.fn(interpolation.colIndex, *w, it.bow, it.options.PrevRow)
		if err != nil {
			return nil, err
		}

		buf := bow.NewBuffer(1, colType, true)
		buf.SetOrDrop(0, interpolatedValue)

		seriesSlice[colIndex] = bow.NewSeries(
			w.Bow.GetColName(interpolation.colIndex),
			colType, buf.Value, buf.Valid)
	}

	startBow, err := bow.NewBow(seriesSlice...)
	if err != nil {
		return nil, err
	}

	return bow.AppendBows(startBow, w.Bow)
}
