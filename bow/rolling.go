package bow

import (
	"errors"
	"fmt"
	"sync"
)

type RollingIterator interface {
	Next() (*Window, error)
}

// IntervalRolling provides an iterator over interval-based windows of bow rows.
// `column`: values to place rows in intervals
// `interval`: window length
//
// Todo:
// - handle more than int64 timestamps
// - bound inclusion option (for now it's `[[`)
func (b *bow) IntervalRolling(column int, interval int64, options IntervalRollingOptions) (*IntervalRollingIterator, error) {
	if column > len(b.Schema().Fields())-1 {
		return nil, fmt.Errorf("no column at index %d", column)
	}
	if interval <= 0 {
		return nil, errors.New("strictly positive interval required")
	}
	if options.Offset < 0 {
		return nil, errors.New("positive offset required")
	}

	return &IntervalRollingIterator{
		bow:       b,
		column:    column,
		interval:  interval,
		options:   options,
		currStart: options.Offset,
	}, nil
}

type IntervalRollingOptions struct {
	Offset int64
}

type IntervalRollingIterator struct {
	sync.Mutex

	bow      Bow
	column   int
	interval int64
	options  IntervalRollingOptions

	currStart int64 // e.g. start time
	currIndex int64
}

type Window struct {
	Bow   Bow
	Start int64
	End   int64
}

// Next window if any left.
//
// e.g. `for w, err := iter.Next(); w != nil; {...}`
func (it *IntervalRollingIterator) Next() (*Window, error) {
	it.Lock()
	defer it.Unlock()

	if !it.hasNext() {
		return nil, nil
	}

	start := it.currStart
	end := it.currStart + it.interval - 1

	var firstIndex int64 = -1
	var lastIndex int64 = -1

	var i int64
	for i = it.currIndex; i < it.bow.NumRows(); i++ {
		v := it.bow.GetValue(it.column, int(i))
		ref, ok := v.(int64)
		if !ok {
			return nil, fmt.Errorf("can't cast '%v' to int64", v)
		}

		if ref < start {
			continue
		}
		if ref > end {
			break
		}

		if firstIndex == -1 {
			firstIndex = i
		}
		lastIndex = i
	}

	it.currIndex = i
	it.currStart = end + 1
	var b Bow
	if firstIndex == -1 {
		b = it.bow.NewSlice(0, 0) // empty
	} else {
		b = it.bow.NewSlice(firstIndex, lastIndex+1)
	}
	return &Window{
		Bow:   b,
		Start: start,
		End:   end,
	}, nil
}

func (it *IntervalRollingIterator) hasNext() bool {
	return it.currIndex < it.bow.NumRows() &&
		it.currStart <= it.bow.GetValue(it.column, int(it.bow.NumRows()-1)).(int64)
}
