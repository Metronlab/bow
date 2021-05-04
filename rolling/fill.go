package rolling

import (
	"errors"
	"fmt"
	"github.com/metronlab/bow"
)

// ColumnInterpolationFunc provides a value at the start of `window`.
type ColumnInterpolationFunc func(inputCol int, window Window, fullBow bow.Bow) (interface{}, error)

func NewColumnInterpolation(inputName string, inputTypes []bow.Type, fn ColumnInterpolationFunc) ColumnInterpolation {
	return ColumnInterpolation{
		inputName:  inputName,
		inputTypes: inputTypes,
		fn:         fn,
	}
}

type ColumnInterpolation struct {
	inputName  string
	inputTypes []bow.Type
	fn         ColumnInterpolationFunc

	inputCol int
}

// Fill each window by interpolating its start if missing
func (it *intervalRollingIterator) Fill(interpolations ...ColumnInterpolation) Rolling {
	const logPrefix = "fill: "

	if it.err != nil {
		return it
	}

	itCopy := *it
	fmt.Printf("FILL IN\nintervalRollingIterator\n%+v\ninterpolations\n%+v\n", itCopy, interpolations)
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

	fmt.Printf("FILL OUT\nintervalRollingIterator\n%+v\ninterpolations\n%+v\n", newIt, interpolations)
	return newIt
}

func (it *intervalRollingIterator) indexedInterpolations(interpolations []ColumnInterpolation) (int, []ColumnInterpolation, error) {
	if len(interpolations) == 0 {
		return -1, nil, fmt.Errorf("at least one column interpolation is required")
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
		name, err := it.bow.GetName(it.column)
		if err != nil {
			return -1, nil, err
		}
		return -1, nil, fmt.Errorf("must keep interval column '%s'", name)
	}

	return newIntervalCol, interpolations, nil
}

func (it *intervalRollingIterator) validateInterpolation(interp *ColumnInterpolation, newIndex int) (bool, error) {
	if interp.inputName == "" {
		return false, fmt.Errorf("interpolation %d has no column name", newIndex)
	}
	readIndex, err := it.bow.GetColumnIndex(interp.inputName)
	if err != nil {
		return false, err
	}
	interp.inputCol = readIndex

	var typeOk bool
	typ := it.bow.GetType(readIndex)
	for _, inputTyp := range interp.inputTypes {
		if typ == inputTyp {
			typeOk = true
			break
		}
	}
	if !typeOk {
		return false, fmt.Errorf("interpolation accepts types %v, got type %s",
			interp.inputTypes, typ.String())
	}

	return readIndex == it.column, nil
}

func (it *intervalRollingIterator) fillWindows(interpolations []ColumnInterpolation) (bow.Bow, error) {
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

func (it *intervalRollingIterator) fillWindow(interpolations []ColumnInterpolation, w *Window) (bow.Bow, error) {
	var first int64 = -1
	if w.Bow.NumRows() > 0 {
		value, i := w.Bow.GetNextFloat64(it.column, 0)
		if i > -1 {
			first = int64(value)
		}
	}

	// has start: call interpolation anyway for those stateful
	if first == w.Start {
		for _, interp := range interpolations {
			_, err := interp.fn(interp.inputCol, *w, it.bow)
			if err != nil {
				return nil, err
			}
		}
		return w.Bow, nil
	}

	// missing start
	seriess := make([]bow.Series, len(interpolations))
	for writeColIndex, interp := range interpolations {
		typ := w.Bow.GetType(interp.inputCol)
		name, err := w.Bow.GetName(interp.inputCol)
		if err != nil {
			return nil, err
		}

		start, err := interp.fn(interp.inputCol, *w, it.bow)
		if err != nil {
			return nil, err
		}

		buf := bow.NewBuffer(1, typ, true)
		buf.SetOrDrop(0, start)

		seriess[writeColIndex] = bow.NewSeries(name, typ, buf.Value, buf.Valid)
	}

	startBow, err := bow.NewBow(seriess...)
	if err != nil {
		return nil, err
	}

	return bow.AppendBows(startBow, w.Bow)
}
