package rolling

import (
	"errors"
	"fmt"

	"github.com/metronlab/bow"
	"github.com/metronlab/bow/transform"
)

type ColumnAggregation interface {
	InputName() string
	InputIndex() int
	MutateInputIndex(int)

	OutputName() string
	Rename(string) ColumnAggregation
	NeedInclusive() bool

	Type() bow.Type
	Func() ColumnAggregationFunc
	Transform(...transform.Transform) ColumnAggregation
	Transforms() []transform.Transform
	GetReturnType(inputType bow.Type, iterator bow.Type) bow.Type
}

type columnAggregation struct {
	inputName       string
	inputIndex      int
	inclusiveWindow bool

	fn         ColumnAggregationFunc
	transforms []transform.Transform

	outputName string
	typ        bow.Type
}

func NewColumnAggregation(colName string, inclusiveWindow bool, returnedType bow.Type, fn ColumnAggregationFunc) ColumnAggregation {
	return &columnAggregation{
		inputName:       colName,
		inputIndex:      -1,
		inclusiveWindow: inclusiveWindow,
		fn:              fn,
		typ:             returnedType,
	}
}

type ColumnAggregationConstruct func(col string) ColumnAggregation
type ColumnAggregationFunc func(col int, w Window) (interface{}, error)

func (a *columnAggregation) GetReturnType(input, iterator bow.Type) (typ bow.Type) {
	switch a.Type() {
	case bow.Int64, bow.Float64, bow.Bool, bow.String:
		typ = a.Type()
	case bow.InputDependent:
		typ = input
	case bow.IteratorDependent:
		typ = iterator
	default:
		panic(fmt.Sprintf("invalid return type %s", a.Type()))
	}
	return
}

func (a *columnAggregation) InputIndex() int {
	return a.inputIndex
}

func (a *columnAggregation) InputName() string {
	return a.inputName
}

func (a *columnAggregation) MutateInputIndex(i int) {
	a.inputIndex = i
}

func (a *columnAggregation) Type() bow.Type {
	return a.typ
}

func (a *columnAggregation) Func() ColumnAggregationFunc {
	return a.fn
}

func (a *columnAggregation) Transform(transforms ...transform.Transform) ColumnAggregation {
	a2 := *a
	a2.transforms = transforms
	return &a2
}

func (a *columnAggregation) Transforms() []transform.Transform {
	return a.transforms
}

func (a *columnAggregation) OutputName() string {
	return a.outputName
}

func (a *columnAggregation) Rename(name string) ColumnAggregation {
	a2 := *a
	a2.outputName = name
	return &a2
}

func (a *columnAggregation) NeedInclusive() bool {
	return a.inclusiveWindow
}

// Aggregate each column using a ColumnAggregation
func (it *intervalRollingIterator) Aggregate(aggrs ...ColumnAggregation) Rolling {
	const logPrefix = "aggregate: "

	if it.err != nil {
		return it
	}
	it2 := *it

	newIntervalCol, aggrs, err := it2.indexedAggrs(aggrs)
	if err != nil {
		return it2.setError(errors.New(logPrefix + err.Error()))
	}

	seriess, err := it2.aggrWindows(aggrs)
	if err != nil {
		return it2.setError(errors.New(logPrefix + err.Error()))
	}

	b, err := bow.NewBow(seriess...)
	if err != nil {
		return it2.setError(errors.New(logPrefix + err.Error()))
	}

	newIt, err := IntervalRollingForIndex(b, newIntervalCol, it2.interval, it2.options)
	if err != nil {
		return it2.setError(errors.New(logPrefix + err.Error()))
	}
	return newIt
}

func (it *intervalRollingIterator) indexedAggrs(aggrs []ColumnAggregation) (int, []ColumnAggregation, error) {
	if len(aggrs) == 0 {
		return -1, nil, fmt.Errorf("at least one column aggregation is required")
	}

	newIntervalCol := -1
	for i := range aggrs {
		isInterval, err := it.validateAggr(aggrs[i], i)
		if err != nil {
			return -1, nil, err
		}
		if isInterval {
			newIntervalCol = i
		}
	}

	if newIntervalCol == -1 {
		name, err := it.bow.GetName(it.column)
		if err != nil {
			return -1, nil, err
		}
		return -1, nil, fmt.Errorf("must keep interval column '%s'", name)
	}

	return newIntervalCol, aggrs, nil
}

func (it *intervalRollingIterator) validateAggr(aggr ColumnAggregation, newIndex int) (isInterval bool, err error) {
	if aggr.InputName() == "" {
		return false, fmt.Errorf("aggregation %d has no column name", newIndex)
	}
	readIndex, err := it.bow.GetColumnIndex(aggr.InputName())
	if err != nil {
		return false, err
	}
	aggr.MutateInputIndex(readIndex)

	if aggr.NeedInclusive() {
		it.options.Inclusive = true
	}

	return readIndex == it.column, nil
}

// For each column aggregation, gives a series with each point resulting from a window aggregation.
func (it *intervalRollingIterator) aggrWindows(aggrs []ColumnAggregation) ([]bow.Series, error) {
	seriess := make([]bow.Series, len(aggrs))
	for writeColIndex, aggr := range aggrs {
		it2 := *it

		buf, outputType, err := it2.windowsAggrBuffer(writeColIndex, aggr)
		if err != nil {
			return nil, err
		}

		name := aggr.OutputName()
		if name == "" {
			var err error
			name, err = it2.bow.GetName(aggr.InputIndex())
			if err != nil {
				return nil, err
			}
		}

		seriess[writeColIndex] = bow.NewSeries(name, outputType, buf.Value, buf.Valid)
	}

	return seriess, nil
}

func (it *intervalRollingIterator) windowsAggrBuffer(colIndex int, aggr ColumnAggregation) (*bow.Buffer, bow.Type, error) {
	var buf bow.Buffer
	var typ bow.Type

	switch aggr.Type() {
	case bow.Int64, bow.Float64, bow.Bool:
		buf = bow.NewBuffer(it.numWindows, aggr.Type(), true)
		typ = aggr.Type()
	case bow.InputDependent:
		cType := it.bow.GetType(aggr.InputIndex())
		buf = bow.NewBuffer(it.numWindows, cType, true)
		typ = cType
	case bow.IteratorDependent:
		iType := it.bow.GetType(it.column)
		buf = bow.NewBuffer(it.numWindows, iType, true)
		typ = iType
	default:
		return nil, bow.Unknown, fmt.Errorf("aggregation %d has invalid return type %s", colIndex, aggr.Type())
	}

	for it.HasNext() {
		winIndex, w, err := it.Next()
		if err != nil {
			return nil, bow.Unknown, err
		}

		var val interface{}
		if !aggr.NeedInclusive() && w.IsInclusive {
			val, err = aggr.Func()(aggr.InputIndex(), (*w).UnsetInclusive())
		} else {
			val, err = aggr.Func()(aggr.InputIndex(), *w)
		}
		if err != nil {
			return nil, bow.Unknown, err
		}
		for _, transform := range aggr.Transforms() {
			val, err = transform(val)
			if err != nil {
				return nil, bow.Unknown, err
			}
		}
		if val == nil {
			continue
		}

		buf.SetOrDrop(winIndex, val)
	}

	return &buf, typ, nil
}
