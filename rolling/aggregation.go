package rolling

import (
	"fmt"

	"github.com/metronlab/bow"
	"github.com/metronlab/bow/transform"
)

type ColAggregation interface {
	InputName() string
	InputIndex() int
	MutateInputIndex(int)

	OutputName() string
	Rename(string) ColAggregation
	NeedInclusive() bool

	Type() bow.Type
	Func() ColAggregationFunc
	Transform(...transform.Transform) ColAggregation
	Transforms() []transform.Transform
	GetReturnType(inputType bow.Type, iterator bow.Type) bow.Type
}

type colAggregation struct {
	inputName       string
	inputIndex      int
	inclusiveWindow bool

	fn         ColAggregationFunc
	transforms []transform.Transform

	outputName string
	typ        bow.Type
}

func NewColAggregation(colName string, inclusiveWindow bool, returnedType bow.Type, fn ColAggregationFunc) ColAggregation {
	return &colAggregation{
		inputName:       colName,
		inputIndex:      -1,
		inclusiveWindow: inclusiveWindow,
		fn:              fn,
		typ:             returnedType,
	}
}

type ColAggregationConstruct func(colName string) ColAggregation
type ColAggregationFunc func(colIndex int, w Window) (interface{}, error)

func (a *colAggregation) GetReturnType(input, iterator bow.Type) (typ bow.Type) {
	switch a.Type() {
	case bow.Int64, bow.Float64, bow.Boolean, bow.String:
		typ = a.Type()
	case bow.InputDependent:
		typ = input
	case bow.IteratorDependent:
		typ = iterator
	default:
		panic(fmt.Errorf("invalid return type %v", a.Type()))
	}
	return
}

func (a *colAggregation) InputIndex() int {
	return a.inputIndex
}

func (a *colAggregation) InputName() string {
	return a.inputName
}

func (a *colAggregation) MutateInputIndex(i int) {
	a.inputIndex = i
}

func (a *colAggregation) Type() bow.Type {
	return a.typ
}

func (a *colAggregation) Func() ColAggregationFunc {
	return a.fn
}

func (a *colAggregation) Transform(transforms ...transform.Transform) ColAggregation {
	a2 := *a
	a2.transforms = transforms
	return &a2
}

func (a *colAggregation) Transforms() []transform.Transform {
	return a.transforms
}

func (a *colAggregation) OutputName() string {
	return a.outputName
}

func (a *colAggregation) Rename(name string) ColAggregation {
	a2 := *a
	a2.outputName = name
	return &a2
}

func (a *colAggregation) NeedInclusive() bool {
	return a.inclusiveWindow
}

// Aggregate each column using a ColAggregation
func (it *intervalRollingIter) Aggregate(aggrs ...ColAggregation) Rolling {
	if it.err != nil {
		return it
	}

	itCopy := *it
	newIntervalCol, aggrs, err := itCopy.indexedAggregations(aggrs)
	if err != nil {
		return itCopy.setError(fmt.Errorf("rolling.Aggregate error: %w", err))
	}

	seriesSlice, err := itCopy.aggregateWindows(aggrs)
	if err != nil {
		return itCopy.setError(fmt.Errorf("rolling.Aggregate error: %w", err))
	}

	b, err := bow.NewBow(seriesSlice...)
	if err != nil {
		return itCopy.setError(fmt.Errorf("rolling.Aggregate error: %w", err))
	}

	itNew, err := IntervalRollingForIndex(b, newIntervalCol, itCopy.interval, itCopy.options)
	if err != nil {
		return itCopy.setError(fmt.Errorf("rolling.Aggregate error: %w", err))
	}

	return itNew
}

func (it *intervalRollingIter) indexedAggregations(aggrs []ColAggregation) (int, []ColAggregation, error) {
	if len(aggrs) == 0 {
		return -1, nil, fmt.Errorf("at least one column aggregation is required")
	}

	newIntervalCol := -1
	for i := range aggrs {
		isInterval, err := it.validateAggregation(aggrs[i], i)
		if err != nil {
			return -1, nil, err
		}
		if isInterval {
			newIntervalCol = i
		}
	}

	if newIntervalCol == -1 {
		return -1, nil, fmt.Errorf("must keep interval column '%s'", it.bow.ColumnName(it.colIndex))
	}

	return newIntervalCol, aggrs, nil
}

func (it *intervalRollingIter) validateAggregation(aggr ColAggregation, newIndex int) (isInterval bool, err error) {
	if aggr.InputName() == "" {
		return false, fmt.Errorf("aggregation %d has no column name", newIndex)
	}
	readIndex, err := it.bow.ColumnIndex(aggr.InputName())
	if err != nil {
		return false, err
	}
	aggr.MutateInputIndex(readIndex)

	if aggr.NeedInclusive() {
		it.options.Inclusive = true
	}

	return readIndex == it.colIndex, nil
}

// For each colIndex aggregation, gives a series with each point resulting from a window aggregation.
func (it *intervalRollingIter) aggregateWindows(aggrs []ColAggregation) ([]bow.Series, error) {
	seriesSlice := make([]bow.Series, len(aggrs))

	var err error
	for colIndex, aggregation := range aggrs {
		itCopy := *it

		colName := aggregation.OutputName()
		if colName == "" {
			colName = itCopy.bow.ColumnName(aggregation.InputIndex())
		}

		seriesSlice[colIndex], err = itCopy.windowsAggregateBuffer(colName, colIndex, aggregation)
		if err != nil {
			return nil, err
		}
	}

	return seriesSlice, nil
}

func (it *intervalRollingIter) windowsAggregateBuffer(colName string, colIndex int, aggr ColAggregation) (bow.Series, error) {
	var series bow.Series

	switch aggr.Type() {
	case bow.Int64, bow.Float64, bow.Boolean:
		series = bow.NewSeriesEmpty(colName, it.numWindows, aggr.Type())
	case bow.InputDependent:
		cType := it.bow.ColumnType(aggr.InputIndex())
		series = bow.NewSeriesEmpty(colName, it.numWindows, cType)
	case bow.IteratorDependent:
		iType := it.bow.ColumnType(it.colIndex)
		series = bow.NewSeriesEmpty(colName, it.numWindows, iType)
	default:
		return series, fmt.Errorf(
			"aggregation %d has invalid return type %s", colIndex, aggr.Type())
	}

	for it.HasNext() {
		winIndex, w, err := it.Next()
		if err != nil {
			return series, err
		}

		var val interface{}
		if !aggr.NeedInclusive() && w.IsInclusive {
			val, err = aggr.Func()(aggr.InputIndex(), (*w).UnsetInclusive())
		} else {
			val, err = aggr.Func()(aggr.InputIndex(), *w)
		}
		if err != nil {
			return series, err
		}
		for _, trans := range aggr.Transforms() {
			val, err = trans(val)
			if err != nil {
				return series, err
			}
		}
		if val == nil {
			continue
		}

		series.SetOrDrop(winIndex, val)
	}

	return series, nil
}
