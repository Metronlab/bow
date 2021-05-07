package rolling

import (
	"errors"
	"fmt"

	"github.com/metronlab/bow"
)

// Rolling allows to process a bow via windows.
// Use `Fill` and/or `Aggregate` to transform windows.
// Use `HasNext` and `Next` to iterate over windows.
// Use `Bow` to get the processed bow.
type Rolling interface {
	Fill(interpolations ...ColInterpolation) Rolling
	Aggregate(aggregations ...ColumnAggregation) Rolling

	NumWindows() (int, error)
	HasNext() bool
	Next() (windowIndex int, w *Window, err error)

	Bow() (bow.Bow, error)
}

type Options struct {
	// offsets windows' start, starting earlier if necessary to preserve first points
	Offset    int64
	Inclusive bool
	PrevRow   bow.Bow
}

func NumWindowsInRange(first, last, interval, offset int64) (int, error) {
	if first > last {
		return -1, errors.New("first must be <= last")
	}
	var err error
	offset, err = validateIntervalOffset(interval, offset)
	if err != nil {
		return -1, err
	}

	start := (first/interval)*interval + offset
	if start > first {
		start -= interval
	}

	return int((last-start)/interval + 1), nil
}

// IntervalRolling provides an interval-based `Rolling`.
// Intervals rely on numerical values regardless of a unit.
// All windows except the last one may be empty.
// `colIndex`: colIndex used to make intervals
// `interval`: length of an interval
func IntervalRolling(b bow.Bow, colName string, interval int64, options Options) (Rolling, error) {
	var err error
	options.PrevRow, err = ValidatePrevRow(b, options.PrevRow)
	if err != nil {
		return nil, fmt.Errorf("rolling.IntervalRolling: %w", err)
	}

	colIndex, err := b.GetColumnIndex(colName)
	if err != nil {
		return nil, fmt.Errorf("rolling.IntervalRolling: %w", err)
	}

	return IntervalRollingForIndex(b, colIndex, interval, options)
}

func IntervalRollingForIndex(b bow.Bow, colIndex int, interval int64, options Options) (Rolling, error) {
	var err error
	options.PrevRow, err = ValidatePrevRow(b, options.PrevRow)
	if err != nil {
		return nil, fmt.Errorf("rolling.IntervalRolling: %w", err)
	}

	options.Offset, err = validateIntervalOffset(interval, options.Offset)
	if err != nil {
		return nil, err
	}

	colType := b.GetType(colIndex)
	if colType != bow.Int64 {
		return nil, fmt.Errorf("rolling.IntervalRolling: impossible to roll over type %v", colType)
	}

	var start int64
	if b.NumRows() > 0 {
		first, valid := b.GetInt64(colIndex, 0)
		if !valid {
			v := b.GetValue(colIndex, 0)
			return nil, fmt.Errorf("rolling.IntervalRolling: expected int64 start value, got %v", v)
		}
		// align first window start on interval
		start = (first/interval)*interval + options.Offset
		if start > first {
			start -= interval
		}
	}

	numWins, err := numWindows(b, colIndex, start, interval)
	if err != nil {
		return nil, err
	}

	return &intervalRollingIter{
		bow:        b,
		colIndex:   colIndex,
		interval:   interval,
		options:    options,
		numWindows: numWins,
		currStart:  start,
	}, nil
}

func ValidatePrevRow(b, prevRow bow.Bow) (bow.Bow, error) {
	if prevRow != nil {
		if b.NumCols() != prevRow.NumCols() {
			return nil, fmt.Errorf("ValidatePrevRow: b and prevRow must have the number of columns")
		}

		for fieldIndex, field := range b.Schema().Fields() {
			if field.Name != prevRow.Schema().Field(fieldIndex).Name {
				return nil, fmt.Errorf("ValidatePrevRow: b and prevRow must have the same field names")
			}
		}

		if prevRow.NumRows() == 0 {
			prevRow = nil
		} else if prevRow.NumRows() != 1 {
			return nil, fmt.Errorf("ValidatePrevRow: prevRow must have only one row, have %d", prevRow.NumRows())
		}
	}

	return prevRow, nil
}

func validateIntervalOffset(interval, offset int64) (int64, error) {
	if interval <= 0 {
		return -1, errors.New("rolling.IntervalRolling: strictly positive interval required")
	}
	if offset >= interval || offset <= -interval {
		offset = offset % interval
	}
	if offset < 0 {
		offset += interval
	}
	return offset, nil
}

type intervalRollingIter struct {
	// todo: sync.Mutex

	bow        bow.Bow
	colIndex   int
	interval   int64
	options    Options
	numWindows int

	currStart   int64 // e.g. start time
	currIndex   int
	windowIndex int
	err         error
}

func (it *intervalRollingIter) Bow() (bow.Bow, error) {
	return it.bow, it.err
}

// HasNext checks if `Next` will provide a window.
//
// todo: concurrent-safe
func (it *intervalRollingIter) HasNext() bool {
	if it.currIndex >= it.bow.NumRows() {
		return false
	}
	n, valid := it.bow.GetInt64(it.colIndex, it.bow.NumRows()-1)
	return valid && it.currStart <= n
}

// Next window if any.
// This mutates the iterator.
//
// todo: concurrent-safe
func (it *intervalRollingIter) Next() (windowIndex int, w *Window, err error) {
	if !it.HasNext() {
		return it.windowIndex, nil, nil
	}

	start := it.currStart
	end := it.currStart + it.interval // include last position even if last point is excluded

	firstIndex, lastIndex := it.currIndex, -1
	var i int
	var isInclusive bool
	for i = firstIndex; i < it.bow.NumRows(); i++ {
		ref, ok := it.bow.GetInt64(it.colIndex, i)
		if !ok {
			continue
		}
		if ref < start {
			continue
		}
		if ref > end {
			break
		}

		if ref == end {
			if isInclusive {
				break
			}
			if !it.options.Inclusive {
				break
			}
			isInclusive = true
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
	if lastIndex == -1 {
		b = it.bow.NewEmpty()
	} else {
		b = it.bow.Slice(firstIndex, lastIndex+1)
	}

	return windowIndex, &Window{
		FirstIndex:          firstIndex,
		Bow:                 b,
		IntervalColumnIndex: it.colIndex,
		Start:               start,
		End:                 end,
		IsInclusive:         isInclusive,
	}, nil
}

func (it *intervalRollingIter) setError(err error) Rolling {
	it.err = err
	return it
}

// NumWindows gives the total of windows across the entire bow this iterator was built from.
func (it *intervalRollingIter) NumWindows() (int, error) {
	return it.numWindows, it.err
}

func numWindows(b bow.Bow, colIndex int, start, interval int64) (int, error) {
	nrows := b.NumRows()
	if nrows == 0 {
		return nrows, nil
	}

	last, irow := b.GetPreviousInt64(colIndex, nrows-1)

	if irow == -1 || start > last {
		return 0, nil
	}

	return int((last-start)/interval + 1), nil
}
