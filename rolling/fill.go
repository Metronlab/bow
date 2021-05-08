package rolling

import (
	"errors"
	"fmt"
	"github.com/metronlab/bow"
)

// ColInterpolationFunc provides a value at the start of `window`.
type ColInterpolationFunc func(inputCol int, window Window, fullBow, prevRow bow.Bow) (interface{}, error)

func NewColumnInterpolation(colName string, inputTypes []bow.Type, fn ColInterpolationFunc) ColumnInterpolation {
	return ColumnInterpolation{
		colName:    colName,
		inputTypes: inputTypes,
		fn:         fn,
	}
}

type ColumnInterpolation struct {
	colName    string
	inputTypes []bow.Type
	fn         ColInterpolationFunc

	colIndex int
}

// Fill each window by interpolating its start if missing
func (it *intervalRollingIter) Fill(interpolations ...ColumnInterpolation) Rolling {
	const logPrefix = "fill: "

	if it.err != nil {
		return it
	}

	itCopy := *it
	newIntervalCol, interpolations, err := itCopy.indexedInterpolations(interpolations)
	if err != nil {
		return itCopy.setError(errors.New(logPrefix + err.Error()))
	}

	b, err := itCopy.fillWindows(interpolations)
	if err != nil {
		return itCopy.setError(errors.New(logPrefix + err.Error()))
	}
	if b == nil {
		b = it.bow.NewEmpty()
	}

	newIt, err := IntervalRollingForIndex(b, newIntervalCol, itCopy.interval, itCopy.options)
	if err != nil {
		return itCopy.setError(errors.New(logPrefix + err.Error()))
	}

	return newIt
}

func (it *intervalRollingIter) indexedInterpolations(interpolations []ColumnInterpolation) (int, []ColumnInterpolation, error) {
	if len(interpolations) == 0 {
		return -1, nil, fmt.Errorf("at least one colIndex interpolation is required")
	}

	newIntervalCol := -1
	for i := range interpolations {
		isInterval, err := it.validateInterpolation(&interpolations[i], i)
		if err != nil {
			return -1, nil, err
		}
		if isInterval {
			newIntervalCol = i
		}
	}

	if newIntervalCol == -1 {
		name, err := it.bow.GetName(it.colIndex)
		if err != nil {
			return -1, nil, err
		}
		return -1, nil, fmt.Errorf("must keep interval column '%s'", name)
	}

	return newIntervalCol, interpolations, nil
}

func (it *intervalRollingIter) validateInterpolation(interpolation *ColumnInterpolation, newIndex int) (bool, error) {
	if interpolation.colName == "" {
		return false, fmt.Errorf("interpolation %d has no colIndex name", newIndex)
	}
	readIndex, err := it.bow.GetColumnIndex(interpolation.colName)
	if err != nil {
		return false, err
	}
	interpolation.colIndex = readIndex

	var typeOk bool
	typ := it.bow.GetType(readIndex)
	for _, inputTyp := range interpolation.inputTypes {
		if typ == inputTyp {
			typeOk = true
			break
		}
	}
	if !typeOk {
		return false, fmt.Errorf("interpolation accepts types %v, got type %s",
			interpolation.inputTypes, typ.String())
	}

	return readIndex == it.colIndex, nil
}

func (it *intervalRollingIter) fillWindows(interpolations []ColumnInterpolation) (bow.Bow, error) {
	it2 := *it

	bows := make([]bow.Bow, it2.numWindows)

	for it2.HasNext() {
		winIndex, w, err := it2.Next()
		if err != nil {
			return nil, err
		}

		bows[winIndex], err = it2.fillWindow(interpolations, w)
		if err != nil {
			return nil, err
		}
	}

	return bow.AppendBows(bows...)
}

func (it *intervalRollingIter) fillWindow(interpolations []ColumnInterpolation, w *Window) (bow.Bow, error) {
	var firstBowValue int64 = -1
	if w.Bow.NumRows() > 0 {
		firstVal, i := w.Bow.GetNextFloat64(it.colIndex, 0)
		if i > -1 {
			firstBowValue = int64(firstVal)
		}
	}

	// has start: call interpolation anyway for those stateful
	if firstBowValue == w.Start {
		for _, interpolation := range interpolations {
			_, err := interpolation.fn(interpolation.colIndex, *w, it.bow, it.options.PrevRow)
			if err != nil {
				return nil, err
			}
		}

		return w.Bow, nil
	}

	// missing start
	seriesSlice := make([]bow.Series, len(interpolations))
	for colIndex, interpolation := range interpolations {
		typ := w.Bow.GetType(interpolation.colIndex)
		name, err := w.Bow.GetName(interpolation.colIndex)
		if err != nil {
			return nil, err
		}

		interpolatedValue, err := interpolation.fn(interpolation.colIndex, *w, it.bow, it.options.PrevRow)
		if err != nil {
			return nil, err
		}

		buf := bow.NewBuffer(1, typ, true)
		buf.SetOrDrop(0, interpolatedValue)

		seriesSlice[colIndex] = bow.NewSeries(name, typ, buf.Value, buf.Valid)
	}

	startBow, err := bow.NewBow(seriesSlice...)
	if err != nil {
		return nil, err
	}

	return bow.AppendBows(startBow, w.Bow)
}
