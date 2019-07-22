package rolling

import (
	"errors"
	"fmt"

	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
)

type ColumnInterpolationFunc func(inputCol int, neededPos int64, window bow.Window, fullBow bow.Bow) (interface{}, error)

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

func (it *intervalRollingIterator) Fill(interpolations ...ColumnInterpolation) Rolling {
	const logPrefix = "fill: "

	if it.err != nil {
		return it
	}
	it2 := *it

	newIntervalCol, interpolations, err := it2.indexedInterpolations(interpolations)
	if err != nil {
		return it2.setError(errors.New(logPrefix + err.Error()))
	}

	bows, err := it2.fillWindows(interpolations)
	if err != nil {
		return it2.setError(errors.New(logPrefix + err.Error()))
	}

	b, err := bow.AppendBows(bows...)
	if err != nil {
		return it2.setError(errors.New(logPrefix + err.Error()))
	}
	if b == nil {
		b = it.bow.NewEmpty()
	}

	newIt, err := IntervalRollingForIndex(b, newIntervalCol, it2.interval, it2.options)
	if err != nil {
		return it2.setError(errors.New(logPrefix + err.Error()))
	}
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
	readIndex, err := it.bow.GetIndex(interp.inputName)
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

func (it *intervalRollingIterator) fillWindows(interpolations []ColumnInterpolation) ([]bow.Bow, error) {
	it2 := *it

	bows := make([]bow.Bow, it2.numWindows)

	for it2.HasNext() {
		winIndex, w, err := it2.Next()
		if err != nil {
			return nil, err
		}

		seriess, err := it2.fillWindowSeriess(interpolations, w, w.Start)
		if err != nil {
			return nil, err
		}

		bows[winIndex], err = bow.NewBow(seriess...)
		if err != nil {
			return nil, err
		}
	}

	return bows, nil
}

func (it *intervalRollingIterator) fillWindowSeriess(interpolations []ColumnInterpolation, w *bow.Window, neededPos int64) ([]bow.Series, error) {
	seriess := make([]bow.Series, len(interpolations))

	var first int64 = -1
	if w.Bow.NumRows() > 0 {
		value, valid := w.Bow.GetInt64(it.column, 0)
		if valid {
			first = value
		}
	}

	startIsMissing := first != w.Start

	for writeColIndex, interp := range interpolations {
		typ := w.Bow.GetType(interp.inputCol)
		name, err := w.Bow.GetName(interp.inputCol)
		if err != nil {
			return nil, err
		}

		bufSize := w.Bow.NumRows()
		if startIsMissing {
			bufSize++
		}
		buf := bow.NewBuffer(bufSize, typ, true)

		rowIndex := 0
		if startIsMissing {
			start, err := interp.fn(interp.inputCol, neededPos, *w, it.bow)
			if err != nil {
				return nil, err
			}
			buf.SetOrDrop(rowIndex, start)
			rowIndex++
		}
		for exRowIndex := 0; exRowIndex < w.Bow.NumRows(); exRowIndex++ {
			buf.SetOrDrop(exRowIndex+rowIndex, w.Bow.GetValue(interp.inputCol, exRowIndex))
		}

		seriess[writeColIndex] = bow.NewSeries(name, typ, buf.Value, buf.Valid)
	}

	return seriess, nil
}
