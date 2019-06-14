package bow

import (
	"errors"
	"fmt"
)

// Rolling allows to process a windowed bow to produce a bow.
// Chain `Fill` and `Aggregate` calls to declare operations on windows.
type Rolling interface {
	Fill() Rolling // todo
	Aggregate(...ColumnAggregation) Rolling
	Bow() (Bow, error)
}

type RollingOptions struct {
	Offset int64
}

// IntervalRolling provides an interval-based `Rolling`.
// `column`: dimension of interval
// `interval`: length of interval
//
// Todo:
// - bound inclusion option (for now it's `[[`)
// - handle more than int64 intervals
func (b *bow) IntervalRolling(column int, interval int64, options RollingOptions) (Rolling, error) {
	if column > len(b.Schema().Fields())-1 {
		return nil, fmt.Errorf("no column at index %d", column)
	}
	if interval <= 0 {
		return nil, errors.New("strictly positive interval required")
	}
	if options.Offset < 0 {
		return nil, errors.New("positive offset required")
	}

	numWins, err := numWindows(b, column, interval, options.Offset)
	if err != nil {
		return nil, err
	}

	var start int64
	if b.NumRows() > 0 {
		v := b.GetValue(column, 0)
		var ok bool
		start, ok = v.(int64)
		if !ok {
			return nil, fmt.Errorf("could not cast %v to int64", v)
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
	interval   int64
	options    RollingOptions
	numWindows int64

	currStart   int64 // e.g. start time
	currIndex   int64
	windowIndex int64
	err         error
}

type Window struct {
	Bow   Bow
	Start int64
	End   int64
}

func (it *intervalRollingIterator) Bow() (Bow, error) {
	return it.bow, it.err
}

// todo
func (it *intervalRollingIterator) Fill() Rolling {
	if it.err != nil {
		return it
	}
	return it
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

	columns := make([][]interface{}, len(aggrs))
	seriess := make([]Series, len(aggrs))

	for writeIndex, aggr := range aggrs {
		it3 := it2

		switch aggr.Type() {
		case Int64, Float64, Bool:
			columns[writeIndex] = make([]interface{}, it3.numWindows)
		default:
			return it3.setError(fmt.Errorf(
				logPrefix+"aggregation %d has invalid return type %s",
				writeIndex, aggr.Type()))
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

			var ok bool
			switch aggr.Type() {
			case Int64:
				columns[writeIndex][winIndex], ok = val.(int64)
			case Float64:
				columns[writeIndex][winIndex], ok = val.(float64)
			case Bool:
				columns[writeIndex][winIndex], ok = val.(bool)
			}
			if !ok {
				return it3.setError(fmt.Errorf(
					logPrefix+"aggregation %d should return %s, returned %t, value: %v",
					writeIndex, aggr.Type(), val, val))
			}
		}

		name := aggr.OutputName()
		if name == "" {
			var err error
			name, err = it3.bow.GetName(aggr.InputIndex())
			if err != nil {
				return it3.setError(errors.New(logPrefix + err.Error()))
			}
		}
		series, err := NewSeriesFromInterfaces(name, aggr.Type(), columns[writeIndex])
		if err != nil {
			return it3.setError(errors.New(logPrefix + err.Error()))
		}
		seriess[writeIndex] = series
	}

	b, err := NewBow(seriess...)
	if err != nil {
		return it2.setError(errors.New(logPrefix + err.Error()))
	}
	r, err := b.IntervalRolling(newIntervalColumn, it2.interval, it2.options)
	if err != nil {
		return it2.setError(errors.New(logPrefix + err.Error()))
	}
	return r
}

func (it *intervalRollingIterator) hasNext() bool {
	return it.currIndex < it.bow.NumRows() &&
		it.currStart <= it.bow.GetValue(it.column, int(it.bow.NumRows()-1)).(int64)
}

func (it *intervalRollingIterator) next() (windowIndex int64, w *Window, err error) {
	if !it.hasNext() {
		return it.windowIndex, nil, nil
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
			return it.windowIndex, nil, fmt.Errorf("can't cast '%v' to int64 to handle value in interval", v)
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
	windowIndex = it.windowIndex
	it.windowIndex++

	var b Bow
	if firstIndex == -1 {
		b = it.bow.NewSlice(0, 0) // empty
	} else {
		b = it.bow.NewSlice(firstIndex, lastIndex+1)
	}
	return windowIndex, &Window{
		Bow:   b,
		Start: start,
		End:   end,
	}, nil
}

func (it *intervalRollingIterator) setError(err error) Rolling {
	it.err = err
	return it
}

func numWindows(b Bow, column int, interval int64, offset int64) (int64, error) {
	nrows := b.NumRows()
	if nrows == 0 {
		return nrows, nil
	}

	first := b.GetValue(column, 0)
	last := b.GetValue(column, int(nrows-1))

	tfirst, ok := first.(int64)
	if !ok {
		return 0, fmt.Errorf("could not cast %v to int64", first)
	}
	tfirst += offset
	tlast, ok := last.(int64)
	if !ok {
		return 0, fmt.Errorf("could not cast %v to int64", last)
	}
	if tfirst > tlast {
		return 0, nil
	}

	return int64((tlast-tfirst)/interval) + 1, nil
}
