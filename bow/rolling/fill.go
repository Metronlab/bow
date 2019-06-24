package rolling

import (
	"errors"
	"fmt"

	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
)

type ColumnInterpolationFunc func(colIndex int, neededPos float64, w bow.Window, fullBow bow.Bow) (interface{}, error)

func NewColumnInterpolation(colName string, inputTypes []bow.Type, fn ColumnInterpolationFunc) ColumnInterpolation {
	return ColumnInterpolation{
		colName:    colName,
		inputTypes: inputTypes,
		fn:         fn,
	}
}

type ColumnInterpolation struct {
	colName    string
	colIndex   int
	inputTypes []bow.Type
	fn         ColumnInterpolationFunc
}

func (it *intervalRollingIterator) Fill(interpolations ...ColumnInterpolation) Rolling {
	const logPrefix = "fill: "

	if it.err != nil {
		return it
	}
	it2 := *it

	newIntervalColumn, interpolations, err := it2.indexedInterpolations(interpolations)
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

	newIt, err := IntervalRollingForIndex(b, newIntervalColumn, it2.interval, it2.options)
	if err != nil {
		return it2.setError(errors.New(logPrefix + err.Error()))
	}
	return newIt
}

func (it *intervalRollingIterator) indexedInterpolations(interpolations []ColumnInterpolation) (int, []ColumnInterpolation, error) {
	newIntervalColumn := -1
	if len(interpolations) == 0 {
		return 0, nil, fmt.Errorf("at least one column interpolation is required")
	}

	for i, interp := range interpolations {
		if interp.colName == "" {
			return 0, nil, fmt.Errorf("interpolation %d has no column name", i)
		}
		readIndex, err := it.bow.GetIndex(interp.colName)
		if err != nil {
			return 0, nil, err
		}
		interpolations[i].colIndex = readIndex
		if readIndex == it.column {
			newIntervalColumn = i
		}

		var typeOk bool
		t, err := it.bow.GetType(readIndex)
		for _, t2 := range interp.inputTypes {
			if err != nil {
				return 0, nil, err
			}
			typeOk = typeOk || t == t2
		}
		if !typeOk {
			return 0, nil, fmt.Errorf("invalid input type %s, must be one of %v", t.String(), interp.inputTypes)
		}
	}

	if newIntervalColumn == -1 {
		name, err := it.bow.GetName(it.column)
		if err != nil {
			return 0, nil, err
		}
		return 0, nil, fmt.Errorf("must keep interval column '%s'", name)
	}

	return newIntervalColumn, interpolations, nil
}

func (it *intervalRollingIterator) fillWindows(interpolations []ColumnInterpolation) ([]bow.Bow, error) {
	it2 := *it

	var bows []bow.Bow

	for it2.HasNext() {
		_, w, err := it2.Next()
		if err != nil {
			return nil, err
		}

		first := -1.
		if w.Bow.NumRows() > 0 {
			first, _ = w.Bow.GetFloat64(it2.column, 0)
		}

		if first != w.Start {
			b, err := it2.bowForRow(interpolations, w, w.Start)
			if err != nil {
				return nil, err
			}
			bows = append(bows, b)
		}

		bows = append(bows, w.Bow)
	}

	return bows, nil
}

func (it *intervalRollingIterator) bowForRow(interpolations []ColumnInterpolation, w *bow.Window, neededPos float64) (bow.Bow, error) {
	cols := make([][]interface{}, len(interpolations))
	for i, interp := range interpolations {
		var err error
		cols[i] = make([]interface{}, 1) // single row
		cols[i][0], err = interp.fn(interp.colIndex, neededPos, *w, it.bow)
		if err != nil {
			return nil, err
		}
	}
	return w.Bow.NewColumns(cols)
}
