package bow

import (
	"errors"
	"fmt"
)

// Rolling allows to process a windowed bow to produce a bow.
// Chain `Fill` and `Aggregate` calls to declare operations on windows.
type Rolling interface {
	Fill(interval float64, interpolations ...ColumnInterpolation) Rolling
	Aggregate(...ColumnAggregation) Rolling
	Bow() (Bow, error)
}

type RollingOptions struct {
	Offset float64
}

// IntervalRolling provides an interval-based `Rolling`.
// `column`: dimension of interval
// `interval`: length of interval
//
// Todo:
// - bound inclusion option (for now it's `[[`)
// - handle more than int64 intervals
func (b *bow) IntervalRolling(column string, interval float64, options RollingOptions) (Rolling, error) {
	index, err := b.GetIndex(column)
	if err != nil {
		return nil, errors.New("intervalrolling: " + err.Error())
	}
	return b.IntervalRollingForIndex(index, interval, options)
}

func (b *bow) IntervalRollingForIndex(column int, interval float64, options RollingOptions) (Rolling, error) {
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

	bow        Bow
	column     int
	interval   float64
	options    RollingOptions
	numWindows int

	currStart   float64 // e.g. start time
	currIndex   int
	windowIndex int
	err         error
}

type Window struct {
	Bow                 Bow
	IntervalColumnIndex int
	Start               float64
	End                 float64
}

func (it *intervalRollingIterator) Bow() (Bow, error) {
	return it.bow, it.err
}

func (it *intervalRollingIterator) hasNext() bool {
	if it.currIndex >= it.bow.NumRows() {
		return false
	}
	n, _ := it.bow.GetFloat64(it.column, it.bow.NumRows()-1)
	return it.currStart <= n
}

func (it *intervalRollingIterator) next() (windowIndex int, w *Window, err error) {
	if !it.hasNext() {
		return it.windowIndex, nil, nil
	}

	start := it.currStart
	end := it.currStart + it.interval

	firstIndex := -1
	lastIndex := -1

	var i int
	for i = it.currIndex; i < it.bow.NumRows(); i++ {
		ref, _ := it.bow.GetFloat64(it.column, int(i))
		if ref < start {
			continue
		}
		if ref >= end {
			break
		}

		if firstIndex == -1 {
			firstIndex = i
		}
		lastIndex = i
	}

	it.currIndex = i
	it.currStart = end
	windowIndex = it.windowIndex
	it.windowIndex++

	var b Bow
	if firstIndex == -1 {
		b = it.bow.NewSlice(0, 0) // empty
	} else {
		b = it.bow.NewSlice(firstIndex, lastIndex+1)
	}
	return windowIndex, &Window{
		Bow:                 b,
		IntervalColumnIndex: it.column,
		Start:               start,
		End:                 end,
	}, nil
}

func (it *intervalRollingIterator) setError(err error) Rolling {
	it.err = err
	return it
}

func numWindows(b Bow, column int, interval float64, offset float64) (int, error) {
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
