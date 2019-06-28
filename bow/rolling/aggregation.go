package rolling

import (
	"errors"
	"fmt"

	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
)

type ColumnAggregation interface {
	InputName() string
	InputIndex() int
	FromIndex(int) ColumnAggregation

	OutputName() string
	Rename(string) ColumnAggregation
	NeedInclusive() bool

	Type() bow.Type
	Func() ColumnAggregationFunc
}

type columnAggregation struct {
	inputName       string
	inputIndex      int
	inclusiveWindow bool

	fn ColumnAggregationFunc

	outputName string
	typ        bow.Type
}

func NewColumnAggregation(colName string, inclusiveWindow bool, returnedType bow.Type, fn ColumnAggregationFunc) ColumnAggregation {
	return columnAggregation{
		inputName:       colName,
		inputIndex:      -1,
		inclusiveWindow: inclusiveWindow,
		fn:              fn,
		typ:             returnedType,
	}
}

type ColumnAggregationFunc func(col int, w bow.Window) (interface{}, error)

func (a columnAggregation) InputIndex() int {
	return a.inputIndex
}

func (a columnAggregation) InputName() string {
	return a.inputName
}

func (a columnAggregation) FromIndex(i int) ColumnAggregation {
	a.inputIndex = i
	return a
}

func (a columnAggregation) Type() bow.Type {
	return a.typ
}

func (a columnAggregation) Func() ColumnAggregationFunc {
	return a.fn
}

func (a columnAggregation) OutputName() string {
	return a.outputName
}

func (a columnAggregation) Rename(name string) ColumnAggregation {
	a.outputName = name
	return a
}

func (a columnAggregation) NeedInclusive() bool {
	return a.inclusiveWindow
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

		if aggr.NeedInclusive() {
			it2.options.Inclusive = true
		}
	}
	if newIntervalColumn == -1 {
		return it2.setError(fmt.Errorf(logPrefix+"must keep column %d for intervals", it2.column))
	}

	outputSeries := make([]bow.Series, len(aggrs))
	for wColIndex, aggr := range aggrs {
		var outputType bow.Type
		it3 := it2

		var buf bow.Buffer
		switch aggr.Type() {
		case bow.Int64, bow.Float64, bow.Bool:
			buf = bow.NewBuffer(int(it3.numWindows), aggr.Type(), true)
			outputType = aggr.Type()
		case bow.InputDependent:
			cType := it.bow.GetType(aggr.InputIndex())
			buf = bow.NewBuffer(int(it3.numWindows), cType, true)
			outputType = cType
		case bow.IteratorDependent:
			buf = bow.NewBuffer(int(it3.numWindows), it.iType, true)
			outputType = it.iType
		default:
			return it3.setError(fmt.Errorf(
				logPrefix+"aggregation %d has invalid return type %s",
				wColIndex, aggr.Type()))
		}

		for it3.HasNext() {
			winIndex, w, err := it3.Next()
			if err != nil {
				return it3.setError(fmt.Errorf(logPrefix+"%s", err.Error()))
			}

			var val interface{}
			if !aggr.NeedInclusive() && w.IsInclusive {
				val, err = aggr.Func()(aggr.InputIndex(), (*w).UnsetInclusive())
			} else {
				val, err = aggr.Func()(aggr.InputIndex(), *w)
			}
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

		outputSeries[wColIndex] = bow.NewSeries(name, outputType, buf.Value, buf.Valid)
	}

	b, err := bow.NewBow(outputSeries...)
	if err != nil {
		return it2.setError(errors.New(logPrefix + err.Error()))
	}
	r, err := IntervalRollingForIndex(b, newIntervalColumn, it2.interval, it2.options)
	if err != nil {
		return it2.setError(errors.New(logPrefix + err.Error()))
	}
	return r
}
