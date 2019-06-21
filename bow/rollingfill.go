package bow

import (
	"errors"
	"fmt"
)

type ColumnInterpolationMethod string

type ColumnInterpolationFunc func(colIndex int, neededPos float64, w Window, fullBow Bow) (interface{}, error)

func NewColumnInterpolation(colName string, okTypes []Type, fn ColumnInterpolationFunc) ColumnInterpolation {
	return ColumnInterpolation{
		colName: colName,
		okTypes: okTypes,
		fn:      fn,
	}
}

type ColumnInterpolation struct {
	colName  string
	colIndex int
	okTypes  []Type
	fn       ColumnInterpolationFunc
}

func (it *intervalRollingIterator) Fill(interval float64, interpolations ...ColumnInterpolation) Rolling {
	const logPrefix = "fill: "

	if it.err != nil {
		return it
	}
	it2 := *it

	newIntervalColumn, interpolations, err := it2.indexedInterpolations(interval, interpolations)
	if err != nil {
		return it2.setError(errors.New(logPrefix + err.Error()))
	}

	bows, err := it2.fillBowWindows(interval, interpolations)
	if err != nil {
		return it2.setError(errors.New(logPrefix + err.Error()))
	}

	b, err := AppendBows(bows...)
	if err != nil {
		return it2.setError(errors.New(logPrefix + err.Error()))
	}

	newIt, err := b.IntervalRollingForIndex(newIntervalColumn, it2.interval, it2.options)
	if err != nil {
		return it2.setError(errors.New(logPrefix + err.Error()))
	}
	return newIt
}

func (it *intervalRollingIterator) indexedInterpolations(interval float64, interpolations []ColumnInterpolation) (int, []ColumnInterpolation, error) {
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
		// todo: validate against okTypes
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

func (it *intervalRollingIterator) fillBowWindows(interval float64, interpolations []ColumnInterpolation) ([]Bow, error) {
	it2 := *it
	bows := make([]Bow, it2.numWindows)
	replaceNils := true // todo: param

	for it2.hasNext() {
		winIndex, w, err := it2.next()
		if err != nil {
			return nil, err
		}

		winSeries := make([]Series, len(interpolations))
		var writeRowIndex int
		for writeColIndex, interp := range interpolations {

			typ, err := it2.bow.GetType(interp.colIndex)
			if err != nil {
				return nil, fmt.Errorf("invalid type %s", typ)
			}

			actualInterval := interval
			if actualInterval == 0 {
				actualInterval = w.End - w.Start
			}

			colSizeAtMost := int((w.End-w.Start)/actualInterval) + w.Bow.NumRows()
			filledCol := NewBuffer(colSizeAtMost, typ, true)

			writeRowIndex = 0 // keep from outer closure
			var currAvailableIndex int
			// include end to include between last interval step and end
			for neededPos := w.Start; neededPos <= w.End; neededPos += actualInterval {
				var availablePos float64 = -1
				var availableVal interface{}
				if currAvailableIndex < w.Bow.NumRows() {
					availablePos, _ = w.Bow.GetFloat64(it.column, currAvailableIndex)
					availableVal = w.Bow.GetValue(interp.colIndex, currAvailableIndex)
				}
				if writeColIndex == 1 && neededPos >= 31. {
					x := 3
					fmt.Println(x)
				}
				// handle value between interval steps
				if availablePos > -1 && availablePos < float64(neededPos) {
					val := availableVal
					if val == nil && replaceNils {
						var err error
						val, err = interp.fn(writeColIndex, availablePos, *w, it.bow)
						if err != nil {
							if err != nil {
								return nil, err
							}
						}
					}
					filledCol.SetOrDrop(writeRowIndex, val)
					writeRowIndex++
					currAvailableIndex++
					neededPos -= actualInterval // redo position

				} else if neededPos == w.End { // end is included for iteration, not for its value
					break

				} else if availablePos == float64(neededPos) && !(availableVal == nil && replaceNils) { // use existing value at interval step
					filledCol.SetOrDrop(writeRowIndex, availableVal)
					currAvailableIndex++
					writeRowIndex++

				} else { // fill at interval step
					addedVal, err := interp.fn(writeColIndex, neededPos, *w, it.bow)
					if err != nil {
							return nil, err
					}
					filledCol.SetOrDrop(writeRowIndex, addedVal)
					writeRowIndex++
				}
			}

			winSeries[writeColIndex] = NewSeries(interp.colName, typ, filledCol.Value, filledCol.Valid)
		}

		b, err := NewBow(winSeries...)
		if err != nil {
			return nil, err
		}
		bows[winIndex] = b.NewSlice(0, writeRowIndex) // drop extra
	}

	return bows, nil
}
