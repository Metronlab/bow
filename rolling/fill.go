package rolling

import (
	"errors"
	"fmt"
	"github.com/metronlab/bow"
)

// ColInterpolationFunc provides a value at the start of `window`.
type ColInterpolationFunc func(inputCol int, window Window, fullBow, prevRow bow.Bow) (interface{}, error)

func NewColInterpolation(inputName string, inputTypes []bow.Type, fn ColInterpolationFunc) ColInterpolation {
	return ColInterpolation{
		inputName:  inputName,
		inputTypes: inputTypes,
		fn:         fn,
	}
}

type ColInterpolation struct {
	inputName  string
	inputTypes []bow.Type
	fn         ColInterpolationFunc

	inputCol int
}

// Fill each window by interpolating its start if missing
func (it *intervalRollingIter) Fill(interpolations ...ColInterpolation) Rolling {
	const logPrefix = "fill: "

	if it.err != nil {
		return it
	}

	itCopy := *it
	//fmt.Printf("FILL IN\nintervalRollingIterator\n%+v\n%+vinterpolations\n%+v\n", itCopy, itCopy.bow, interpolations)
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
	//fmt.Printf("FILLED WINDOWS\n%+v\n", b)

	newIt, err := IntervalRollingForIndex(b, nil, newIntervalCol, itCopy.interval, itCopy.options)
	if err != nil {
		return itCopy.setError(errors.New(logPrefix + err.Error()))
	}

	//b, _ = newIt.Bow()
	//fmt.Printf("FILL OUT\nintervalRollingIterator\n%+v\n%+vinterpolations\n%+v\n", newIt, b, interpolations)
	return newIt
}

func (it *intervalRollingIter) indexedInterpolations(interpolations []ColInterpolation) (int, []ColInterpolation, error) {
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
		return -1, nil, fmt.Errorf("must keep interval colIndex '%s'", name)
	}

	return newIntervalCol, interpolations, nil
}

func (it *intervalRollingIter) validateInterpolation(interpolation *ColInterpolation, newIndex int) (bool, error) {
	if interpolation.inputName == "" {
		return false, fmt.Errorf("interpolation %d has no colIndex name", newIndex)
	}
	readIndex, err := it.bow.GetColIndex(interpolation.inputName)
	if err != nil {
		return false, err
	}
	interpolation.inputCol = readIndex

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

func (it *intervalRollingIter) fillWindows(interpolations []ColInterpolation) (bow.Bow, error) {
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

func (it *intervalRollingIter) fillWindow(interpolations []ColInterpolation, w *Window) (bow.Bow, error) {
	var first int64 = -1
	if w.Bow.NumRows() > 0 {
		value, i := w.Bow.GetNextFloat64(it.colIndex, 0)
		if i > -1 {
			first = int64(value)
		}
	}

	// has start: call interpolation anyway for those stateful
	if first == w.Start {
		for _, interp := range interpolations {
			_, err := interp.fn(interp.inputCol, *w, it.bow, nil)
			if err != nil {
				return nil, err
			}
		}
		return w.Bow, nil
	}

	// missing start
	seriesSlice := make([]bow.Series, len(interpolations))
	for writeColIndex, interp := range interpolations {
		typ := w.Bow.GetType(interp.inputCol)
		name, err := w.Bow.GetName(interp.inputCol)
		if err != nil {
			return nil, err
		}

		start, err := interp.fn(interp.inputCol, *w, it.bow, it.prevRow)
		if err != nil {
			return nil, err
		}

		buf := bow.NewBuffer(1, typ, true)
		buf.SetOrDrop(0, start)

		seriesSlice[writeColIndex] = bow.NewSeries(name, typ, buf.Value, buf.Valid)
	}

	startBow, err := bow.NewBow(seriesSlice...)
	if err != nil {
		return nil, err
	}

	return bow.AppendBows(startBow, w.Bow)
}
