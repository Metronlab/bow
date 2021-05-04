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
	Func() ColumnAggregationFunc
	Transform(...transform.Transform) ColAggregation
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

func NewColumnAggregation(colName string, inclusiveWindow bool, returnedType bow.Type, fn ColumnAggregationFunc) ColAggregation {
	return &columnAggregation{
		inputName:       colName,
		inputIndex:      -1,
		inclusiveWindow: inclusiveWindow,
		fn:              fn,
		typ:             returnedType,
	}
}

type ColumnAggregationConstruct func(col string) ColAggregation
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

func (a *columnAggregation) Transform(transforms ...transform.Transform) ColAggregation {
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

func (a *columnAggregation) Rename(name string) ColAggregation {
	a2 := *a
	a2.outputName = name
	return &a2
}

func (a *columnAggregation) NeedInclusive() bool {
	return a.inclusiveWindow
}

// Aggregate each colIndex using a ColAggregation
func (it *intervalRollingIter) Aggregate(aggrs ...ColAggregation) Rolling {
	if it.err != nil {
		return it
	}

	itCopy := *it
	newIntervalCol, aggrs, err := itCopy.indexedAggrs(aggrs)
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

	itNew, err := IntervalRollingForIndex(b, nil, newIntervalCol, itCopy.interval, itCopy.options)
	if err != nil {
		return itCopy.setError(fmt.Errorf("rolling.Aggregate error: %w", err))
	}

	b, _ = itNew.Bow()
	fmt.Printf("rolling.Aggregate\nBEFORE:\n%+vAFTER:\n%+v\n", it.bow, b)
	return itNew
}

func (it *intervalRollingIter) indexedAggrs(aggregations []ColAggregation) (int, []ColAggregation, error) {
	if len(aggregations) == 0 {
		return -1, nil, fmt.Errorf("at least one colIndex aggregation is required")
	}

	newIntervalCol := -1
	for i := range aggregations {
		isInterval, err := it.validateAggregation(aggregations[i], i)
		if err != nil {
			return -1, nil, err
		}
		if isInterval {
			newIntervalCol = i
		}
	}

	if newIntervalCol == -1 {
		name, err := it.bow.GetName(it.colIndex)
		if err != nil {
			return -1, nil, err
		}
		return -1, nil, fmt.Errorf("must keep interval colIndex '%s'", name)
	}

	return newIntervalCol, aggregations, nil
}

func (it *intervalRollingIter) validateAggregation(aggregation ColAggregation, newIndex int) (isInterval bool, err error) {
	if aggregation.InputName() == "" {
		return false, fmt.Errorf("aggregation %d has no colIndex name", newIndex)
	}
	readIndex, err := it.bow.GetColIndex(aggregation.InputName())
	if err != nil {
		return false, err
	}
	aggregation.MutateInputIndex(readIndex)

	if aggregation.NeedInclusive() {
		it.options.Inclusive = true
	}

	return readIndex == it.colIndex, nil
}

// For each colIndex aggregation, gives a series with each point resulting from a window aggregation.
func (it *intervalRollingIter) aggregateWindows(aggregations []ColAggregation) ([]bow.Series, error) {
	seriesSlice := make([]bow.Series, len(aggregations))

	for writeColIndex, aggr := range aggregations {
		itCopy := *it

		buf, outputType, err := itCopy.windowsAggrBuffer(writeColIndex, aggr)
		if err != nil {
			return nil, err
		}

		name := aggr.OutputName()
		if name == "" {
			var err error
			name, err = itCopy.bow.GetName(aggr.InputIndex())
			if err != nil {
				return nil, err
			}
		}

		seriesSlice[writeColIndex] = bow.NewSeries(name, outputType, buf.Value, buf.Valid)
	}

	return seriesSlice, nil
}

func (it *intervalRollingIter) windowsAggrBuffer(colIndex int, aggr ColAggregation) (*bow.Buffer, bow.Type, error) {
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
		iType := it.bow.GetType(it.colIndex)
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
		for _, trans := range aggr.Transforms() {
			val, err = trans(val)
			if err != nil {
				return nil, bow.Unknown, err
			}
		}
		if val == nil {
			continue
		}

		buf.SetOrDrop(winIndex, val)
	}

	//fmt.Printf("windowAggrBuffer colIndex %d typ:%s buf:%+v\n", colIndex, typ.String(), buf)

	return &buf, typ, nil
}
