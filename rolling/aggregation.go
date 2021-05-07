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

type ColumnAggregationConstruct func(colName string) ColAggregation
type ColumnAggregationFunc func(colIndex int, w Window) (interface{}, error)

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
func (it *intervalRollingIter) Aggregate(aggregations ...ColAggregation) Rolling {
	if it.err != nil {
		return it
	}

	itCopy := *it
	newIntervalCol, aggregations, err := itCopy.indexedAggregations(aggregations)
	if err != nil {
		return itCopy.setError(fmt.Errorf("rolling.Aggregate error: %w", err))
	}

	seriesSlice, err := itCopy.aggregateWindows(aggregations)
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

	//b, _ = itNew.Bow()
	//fmt.Printf("rolling.Aggregate\nBEFORE:\n%+vAFTER:\n%+v\n", it.bow, b)
	return itNew
}

func (it *intervalRollingIter) indexedAggregations(aggregations []ColAggregation) (int, []ColAggregation, error) {
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
		return -1, nil, fmt.Errorf("must keep interval column '%s'", name)
	}

	return newIntervalCol, aggregations, nil
}

func (it *intervalRollingIter) validateAggregation(aggregation ColAggregation, newIndex int) (isInterval bool, err error) {
	if aggregation.InputName() == "" {
		return false, fmt.Errorf("aggregation %d has no column name", newIndex)
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

	for colIndex, aggregation := range aggregations {
		itCopy := *it

		buf, outputType, err := itCopy.windowsAggregateBuffer(colIndex, aggregation)
		if err != nil {
			return nil, err
		}

		colName := aggregation.OutputName()
		if colName == "" {
			var err error
			colName, err = itCopy.bow.GetName(aggregation.InputIndex())
			if err != nil {
				return nil, err
			}
		}

		seriesSlice[colIndex] = bow.NewSeries(colName, outputType, buf.Value, buf.Valid)
	}

	return seriesSlice, nil
}

func (it *intervalRollingIter) windowsAggregateBuffer(colIndex int, aggregation ColAggregation) (*bow.Buffer, bow.Type, error) {
	var buf bow.Buffer
	var typ bow.Type

	switch aggregation.Type() {
	case bow.Int64, bow.Float64, bow.Bool:
		buf = bow.NewBuffer(it.numWindows, aggregation.Type(), true)
		typ = aggregation.Type()
	case bow.InputDependent:
		cType := it.bow.GetType(aggregation.InputIndex())
		buf = bow.NewBuffer(it.numWindows, cType, true)
		typ = cType
	case bow.IteratorDependent:
		iType := it.bow.GetType(it.colIndex)
		buf = bow.NewBuffer(it.numWindows, iType, true)
		typ = iType
	default:
		return nil, bow.Unknown, fmt.Errorf("aggregation %d has invalid return type %s", colIndex, aggregation.Type())
	}

	for it.HasNextWindow() {
		winIndex, w, err := it.NextWindow()
		if err != nil {
			return nil, bow.Unknown, err
		}

		var val interface{}
		if !aggregation.NeedInclusive() && w.IsInclusive {
			val, err = aggregation.Func()(aggregation.InputIndex(), (*w).UnsetInclusive())
		} else {
			val, err = aggregation.Func()(aggregation.InputIndex(), *w)
		}
		if err != nil {
			return nil, bow.Unknown, err
		}
		for _, trans := range aggregation.Transforms() {
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
