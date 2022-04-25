package rolling

import (
	"errors"
	"fmt"

	"github.com/metronlab/bow"
)

// Rolling enables processing a Bow via windows.
// Use Interpolate() and/or Aggregate() to transform windows.
// Use Next() to iterate over windows.
// Use Bow() to get the processed Bow.
type Rolling interface {
	// Aggregate aggregates each column by using a ColAggregation.
	Aggregate(...ColAggregation) Rolling
	// Interpolate fills each window by interpolating its start if missing.
	Interpolate(...ColInterpolation) Rolling

	// NumWindows returns the total number of windows in the Bow.
	NumWindows() (int, error)
	// HasNext returns true if the next call to Next() will return a new Window.
	HasNext() bool
	// Next returns the next Window, along with its index.
	Next() (windowIndex int, window *Window, err error)

	// Bow returns the Bow from the Rolling.
	Bow() (bow.Bow, error)
}

type intervalRolling struct {
	// TODO: sync.Mutex
	bow              bow.Bow
	intervalColIndex int
	interval         int64
	options          Options
	numWindows       int

	currWindowFirstValue int64
	currRowIndex         int
	currWindowIndex      int
	err                  error
}

// Options sets options for IntervalRolling:
// - Offset: interval to move the window start, can be negative.
// - Inclusive: sets if the window needs to be inclusive; i.e., includes the last point.
// - PrevRow: extra point before the window to enable better interpolation.
type Options struct {
	Offset    int64
	Inclusive bool
	PrevRow   bow.Bow
}

// IntervalRolling returns a new interval-based Rolling with:
// - b: Bow to process in windows
// - colName: column on which the interval is based on
// - interval: numeric value independent of any unit, length of the windows
// All windows except the last one may be empty.
func IntervalRolling(b bow.Bow, colName string, interval int64, options Options) (Rolling, error) {
	colIndex, err := b.ColumnIndex(colName)
	if err != nil {
		return nil, err
	}

	return newIntervalRolling(b, colIndex, interval, options)
}

func newIntervalRolling(b bow.Bow, intervalColIndex int, interval int64, options Options) (Rolling, error) {
	if b.ColumnType(intervalColIndex) != bow.Int64 {
		return nil, fmt.Errorf("impossible to create a new intervalRolling on column of type %v",
			b.ColumnType(intervalColIndex))
	}

	var err error
	options.Offset, err = enforceIntervalAndOffset(interval, options.Offset)
	if err != nil {
		return nil, fmt.Errorf("enforceIntervalAndOffset: %w", err)
	}

	options.PrevRow, err = enforcePrevRow(options.PrevRow)
	if err != nil {
		return nil, fmt.Errorf("enforcePrevRow: %w", err)
	}

	var windowFirstValue int64
	if b.NumRows() > 0 {
		firstBowValue, valid := b.GetInt64(intervalColIndex, 0)
		if !valid {
			return nil, fmt.Errorf(
				"the first value of the column should be convertible to int64, got %v",
				b.GetValue(intervalColIndex, 0))
		}

		// align window first value on interval
		windowFirstValue = (firstBowValue/interval)*interval + options.Offset
		if windowFirstValue > firstBowValue {
			windowFirstValue -= interval
		}
	}

	numWindows := countWindows(b, intervalColIndex, windowFirstValue, interval)

	return &intervalRolling{
		bow:                  b,
		intervalColIndex:     intervalColIndex,
		interval:             interval,
		options:              options,
		numWindows:           numWindows,
		currWindowFirstValue: windowFirstValue,
	}, nil
}

func enforceIntervalAndOffset(interval, offset int64) (int64, error) {
	if interval <= 0 {
		return -1, errors.New("strictly positive interval required")
	}

	if offset >= interval || offset <= -interval {
		offset = offset % interval
	}

	if offset < 0 {
		offset += interval
	}

	return offset, nil
}

func enforcePrevRow(prevRow bow.Bow) (bow.Bow, error) {
	if prevRow == nil || prevRow.NumRows() == 0 {
		return nil, nil
	}

	if prevRow.NumRows() != 1 {
		return nil, fmt.Errorf("prevRow must have only one row, have %d",
			prevRow.NumRows())
	}

	return prevRow, nil
}

func countWindows(b bow.Bow, colIndex int, firstWindowStart, interval int64) int {
	if b.NumRows() == 0 {
		return 0
	}

	lastBowValue, lastBowValueRowIndex := b.GetPrevInt64(colIndex, b.NumRows()-1)
	if lastBowValueRowIndex == -1 || firstWindowStart > lastBowValue {
		return 0
	}

	return int((lastBowValue-firstWindowStart)/interval + 1)
}

func (r *intervalRolling) NumWindows() (int, error) {
	return r.numWindows, r.err
}

// TODO: concurrent-safe

func (r *intervalRolling) HasNext() bool {
	if r.currRowIndex >= r.bow.NumRows() {
		return false
	}

	lastBowValue, lastBowValueIsValid := r.bow.GetInt64(r.intervalColIndex, r.bow.NumRows()-1)
	if !lastBowValueIsValid {
		return false
	}

	return r.currWindowFirstValue <= lastBowValue
}

// TODO: concurrent-safe

func (r *intervalRolling) Next() (windowIndex int, window *Window, err error) {
	if !r.HasNext() {
		return r.currWindowIndex, nil, nil
	}

	firstValue := r.currWindowFirstValue
	lastValue := r.currWindowFirstValue + r.interval // include last position even if last point is excluded

	rowIndex := 0
	isInclusive := false
	firstRowIndex := r.currRowIndex
	lastRowIndex := -1
	for rowIndex = firstRowIndex; rowIndex < r.bow.NumRows(); rowIndex++ {
		val, ok := r.bow.GetInt64(r.intervalColIndex, rowIndex)
		if !ok {
			continue
		}
		if val < firstValue {
			continue
		}
		if val > lastValue {
			break
		}

		if val == lastValue {
			if isInclusive {
				break
			}
			if !r.options.Inclusive {
				break
			}
			isInclusive = true
		}

		lastRowIndex = rowIndex
	}

	if !isInclusive {
		r.currRowIndex = rowIndex
	} else {
		r.currRowIndex = rowIndex - 1
	}

	r.currWindowFirstValue = lastValue
	windowIndex = r.currWindowIndex
	r.currWindowIndex++

	var b bow.Bow
	if lastRowIndex == -1 {
		b = r.bow.NewEmptySlice()
	} else {
		b = r.bow.NewSlice(firstRowIndex, lastRowIndex+1)
	}

	return windowIndex, &Window{
		Bow:              b,
		FirstIndex:       firstRowIndex,
		IntervalColIndex: r.intervalColIndex,
		FirstValue:       firstValue,
		LastValue:        lastValue,
		IsInclusive:      isInclusive,
	}, nil
}

func (r *intervalRolling) Bow() (bow.Bow, error) {
	return r.bow, r.err
}

func (r *intervalRolling) setError(err error) Rolling {
	r.err = err
	return r
}
