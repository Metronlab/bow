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

// Aggregate each column using a ColumnAggregation
func (it *intervalRollingIterator) Aggregate(aggrs ...ColumnAggregation) Rolling {
	const logPrefix = "aggregate: "

	it2 := *it // preserve previous states still referenced

	if it2.err != nil {
		return &it2
	}

	newIntervalColumn := -1
	if len(aggrs) == 0 {
		return it2.setError(fmt.Errorf("at least one column aggregation is required"))
	}
	for i, aggr := range aggrs {
		if aggr.InputName() == "" {
			return it2.setError(fmt.Errorf(logPrefix+"aggregation %d has no column name", i))
		}
		readIndex, err := it2.bow.GetIndex(aggr.InputName())
		if err != nil {
			return it2.setError(fmt.Errorf(logPrefix+"%s", err.Error()))
		}
		aggrs[i] = aggr.FromIndex(readIndex)
		if readIndex == it2.column {
			newIntervalColumn = i
		}
	}
	if newIntervalColumn == -1 {
		return it2.setError(fmt.Errorf(logPrefix+"must keep column %d for intervals", it2.column))
	}

	outputSeries := make([]Series, len(aggrs))
	for wColIndex, aggr := range aggrs {
		it3 := it2

		var buf Buffer
		switch aggr.Type() {
		case Int64, Float64, Bool:
			buf = NewBuffer(int(it3.numWindows), aggr.Type(), true)
		default:
			return it3.setError(fmt.Errorf(
				logPrefix+"aggregation %d has invalid return type %s",
				wColIndex, aggr.Type()))
		}

		for it3.hasNext() {
			winIndex, w, err := it3.next()
			if err != nil {
				return it3.setError(fmt.Errorf(logPrefix+"%s", err.Error()))
			}

			val, err := aggr.Func()(aggr.InputIndex(), *w)
			if err != nil {
				return it3.setError(fmt.Errorf(logPrefix+"%s", err.Error()))
			}
			if val == nil {
				continue
			}

			buf.SetOrDrop(int(winIndex), val)
		}

		name := aggr.OutputName()
		if name == "" {
			var err error
			name, err = it3.bow.GetName(aggr.InputIndex())
			if err != nil {
				return it3.setError(errors.New(logPrefix + err.Error()))
			}
		}

		outputSeries[wColIndex] = NewSeries(name, aggr.Type(), buf.Value, buf.Valid)
	}

	b, err := NewBow(outputSeries...)
	if err != nil {
		return it2.setError(errors.New(logPrefix + err.Error()))
	}
	r, err := b.IntervalRollingForIndex(newIntervalColumn, it2.interval, it2.options)
	if err != nil {
		return it2.setError(errors.New(logPrefix + err.Error()))
	}
	return r
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
