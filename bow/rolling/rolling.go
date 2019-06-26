package rolling

import (
	"errors"
	"fmt"

	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
)

// Rolling allows to process a bow via windows.
// Use `Fill` and/or `Aggregate` to transform windows.
// Use `HasNext` and `Next` to iterate over windows.
// Use `Bow` to join windows into a new bow.
type Rolling interface {
	Fill(interpolations ...ColumnInterpolation) Rolling
	Aggregate(...ColumnAggregation) Rolling

	NumWindows() (int, error)
	HasNext() bool
	Next() (windowIndex int, w *bow.Window, err error)

	Bow() (bow.Bow, error)
}

type Options struct {
	Offset    float64
	Inclusive bool
}

// IntervalRolling provides an interval-based `Rolling`.
// Intervals rely on numerical values regardless of a unit.
// All windows except the last one may be empty.
// `column`: column used to make intervals
// `interval`: length of an interval
//
// Todo:
// - bound inclusion option (for now it's `[[`)
func IntervalRolling(b bow.Bow, column string, interval float64, options Options) (Rolling, error) {
	index, err := b.GetIndex(column)
	if err != nil {
		return nil, errors.New("intervalrolling: " + err.Error())
	}
	return IntervalRollingForIndex(b, index, interval, options)
}

func IntervalRollingForIndex(b bow.Bow, column int, interval float64, options Options) (Rolling, error) {
	logPrefix := "intervalrolling: "

	if interval <= 0 {
		return nil, errors.New(logPrefix + "strictly positive interval required")
	}
	if options.Offset < 0 {
		return nil, errors.New(logPrefix + "positive offset required")
	}

	numWins, err := numWindows(b, column, interval, options.Offset)
	if err != nil {
		return nil, err
	}

	var start float64
	if b.NumRows() > 0 {
		var valid bool
		start, valid = b.GetFloat64(column, 0)
		if !valid {
			return nil, fmt.Errorf(logPrefix+"invalid cast for start value =%d", start)
		}
	}
	start += options.Offset

	return &intervalRollingIterator{
		bow:        b,
		column:     column,
		interval:   interval,
		options:    options,
		numWindows: numWins,
		currStart:  start,
	}, nil
}

type intervalRollingIterator struct {
	// todo: sync.Mutex

	bow        bow.Bow
	column     int
	interval   float64
	options    Options
	numWindows int

	currStart   float64 // e.g. start time
	currIndex   int
	windowIndex int
	err         error
}

func (it *intervalRollingIterator) Bow() (bow.Bow, error) {
	return it.bow, it.err
}

// HasNext checks if `Next` will provide a window.
//
// todo: concurrent-safe
func (it *intervalRollingIterator) HasNext() bool {
	if it.currIndex >= it.bow.NumRows() {
		return false
	}
	n, _ := it.bow.GetFloat64(it.column, it.bow.NumRows()-1)
	return it.currStart <= n
}

// Next window if any.
// This mutates the iterator.
//
// todo: concurrent-safe
func (it *intervalRollingIterator) Next() (windowIndex int, w *bow.Window, err error) {
	if !it.HasNext() {
		return it.windowIndex, nil, nil
	}

	start := it.currStart
	end := it.currStart + it.interval

	firstIndex := -1
	lastIndex := -1

	var i int
	var isInclusive bool
	for i = it.currIndex; i < it.bow.NumRows(); i++ {
		ref, _ := it.bow.GetFloat64(it.column, int(i))
		if ref < start {
			continue
		}
		if ref > end {
			break
		}

		if ref == end {
			if !it.options.Inclusive {
				break
			}
			isInclusive = true
		}

		if firstIndex == -1 {
			firstIndex = i
		}
		lastIndex = i
	}

	if !isInclusive {
		it.currIndex = i
	} else {
		it.currIndex = i - 1
	}
	it.currStart = end
	windowIndex = it.windowIndex
	it.windowIndex++

	var b bow.Bow
	if firstIndex == -1 {
		b = it.bow.NewSlice(0, 0) // empty
	} else {
		b = it.bow.NewSlice(firstIndex, lastIndex+1)
	}
	return windowIndex, &bow.Window{
		Bow:                 b,
		IntervalColumnIndex: it.column,
		Start:               start,
		End:                 end,
		IsInclusive:         isInclusive,
	}, nil
}

func (it *intervalRollingIterator) setError(err error) Rolling {
	it.err = err
	return it
}

// NumWindows gives the total of windows across the entire bow this iterator was built from.
func (it *intervalRollingIterator) NumWindows() (int, error) {
	return it.numWindows, it.err
}

func numWindows(b bow.Bow, column int, interval float64, offset float64) (int, error) {
	nrows := b.NumRows()
	if nrows == 0 {
		return nrows, nil
	}

	tfirst, _ := b.GetFloat64(column, 0)
	tfirst += offset
	tlast, _ := b.GetFloat64(column, nrows-1)

	if tfirst > tlast {
		return 0, nil
	}

	return int((tlast-tfirst)/interval) + 1, nil
}
