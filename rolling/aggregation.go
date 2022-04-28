package rolling

import (
	"fmt"

	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling/transformation"
)

// ColAggregation is a set of methods to aggregate and transform a Window.
type ColAggregation interface {
	// InputName returns the name of the input column.
	InputName() string
	// InputIndex returns the index of the input column.
	InputIndex() int
	// SetInputIndex sets the index of the input column.
	SetInputIndex(int)

	// OutputName returns the name of the output column.
	OutputName() string
	// RenameOutput returns a copy of the ColAggregation with a new output column name.
	RenameOutput(string) ColAggregation
	// NeedInclusiveWindow returns true if the ColAggregation needs to have inclusive windows.
	NeedInclusiveWindow() bool

	// Type returns the return type of the ColAggregation.
	Type() bow.Type
	// GetReturnType returns the return type of the ColAggregation depending on an input and an iterator type.
	GetReturnType(inputType, iteratorType bow.Type) bow.Type

	// Func returns the ColAggregationFunc of the ColAggregation.
	Func() ColAggregationFunc

	// Transformations returns the transformation functions of the ColAggregation.
	Transformations() []transformation.Func
	// SetTransformations returns a copy of the ColAggregation with new transformations functions.
	SetTransformations(...transformation.Func) ColAggregation
}

type colAggregation struct {
	inputName           string
	inputIndex          int
	needInclusiveWindow bool

	aggregationFn     ColAggregationFunc
	transformationFns []transformation.Func

	outputName string
	typ        bow.Type
}

// NewColAggregation returns a new ColAggregation.
func NewColAggregation(inputName string, needInclusiveWindow bool, typ bow.Type, fn ColAggregationFunc) ColAggregation {
	return &colAggregation{
		inputName:           inputName,
		inputIndex:          -1,
		needInclusiveWindow: needInclusiveWindow,
		aggregationFn:       fn,
		typ:                 typ,
	}
}

type ColAggregationConstruct func(colName string) ColAggregation
type ColAggregationFunc func(colIndex int, w Window) (interface{}, error)

func (a *colAggregation) InputName() string {
	return a.inputName
}

func (a *colAggregation) InputIndex() int {
	return a.inputIndex
}

func (a *colAggregation) SetInputIndex(i int) {
	a.inputIndex = i
}

func (a *colAggregation) OutputName() string {
	return a.outputName
}

func (a *colAggregation) RenameOutput(name string) ColAggregation {
	aCopy := *a
	aCopy.outputName = name
	return &aCopy
}

func (a *colAggregation) NeedInclusiveWindow() bool {
	return a.needInclusiveWindow
}

func (a *colAggregation) Type() bow.Type {
	return a.typ
}

func (a *colAggregation) Func() ColAggregationFunc {
	return a.aggregationFn
}

func (a *colAggregation) Transformations() []transformation.Func {
	return a.transformationFns
}

func (a *colAggregation) SetTransformations(transformations ...transformation.Func) ColAggregation {
	aCopy := *a
	aCopy.transformationFns = transformations
	return &aCopy
}

func (a *colAggregation) GetReturnType(inputType, iteratorType bow.Type) bow.Type {
	switch a.Type() {
	case bow.Int64, bow.Float64, bow.Bool, bow.String:
		return a.Type()
	case bow.InputDependent:
		return inputType
	case bow.IteratorDependent:
		return iteratorType
	default:
		panic(fmt.Errorf("invalid return type %v", a.Type()))
	}
}

func (r *intervalRolling) Aggregate(aggrs ...ColAggregation) Rolling {
	if r.err != nil {
		return r
	}

	rCopy := *r
	newIntervalCol, aggrs, err := rCopy.indexedAggregations(aggrs)
	if err != nil {
		return rCopy.setError(fmt.Errorf("intervalRolling.indexedAggregations: %w", err))
	}

	b, err := rCopy.aggregateWindows(aggrs)
	if err != nil {
		return rCopy.setError(fmt.Errorf("intervalRolling.aggregateWindows: %w", err))
	}

	newR, err := newIntervalRolling(b, newIntervalCol, rCopy.interval, rCopy.options)
	if err != nil {
		return rCopy.setError(fmt.Errorf("newIntervalRolling: %w", err))
	}

	return newR
}

func (r *intervalRolling) indexedAggregations(aggrs []ColAggregation) (int, []ColAggregation, error) {
	if len(aggrs) == 0 {
		return -1, nil, fmt.Errorf("at least one column aggregation is required")
	}

	newIntervalCol := -1
	for i := range aggrs {
		isInterval, err := r.validateAggregation(aggrs[i], i)
		if err != nil {
			return -1, nil, err
		}
		if isInterval {
			newIntervalCol = i
		}
	}

	if newIntervalCol == -1 {
		return -1, nil, fmt.Errorf(
			"must keep interval column '%s'", r.bow.ColumnName(r.intervalColIndex))
	}

	return newIntervalCol, aggrs, nil
}

func (r *intervalRolling) validateAggregation(aggr ColAggregation, newIndex int) (isInterval bool, err error) {
	if aggr.InputName() == "" {
		return false, fmt.Errorf("aggregation %d has no column name", newIndex)
	}

	readIndex, err := r.bow.ColumnIndex(aggr.InputName())
	if err != nil {
		return false, err
	}

	aggr.SetInputIndex(readIndex)

	if aggr.NeedInclusiveWindow() {
		r.options.Inclusive = true
	}

	return readIndex == r.intervalColIndex, nil
}

func (r *intervalRolling) aggregateWindows(aggrs []ColAggregation) (bow.Bow, error) {
	series := make([]bow.Series, len(aggrs))

	for colIndex, aggr := range aggrs {
		rCopy := *r
		typ := aggr.GetReturnType(
			rCopy.bow.ColumnType(aggr.InputIndex()),
			rCopy.bow.ColumnType(rCopy.intervalColIndex))
		buf := bow.NewBuffer(rCopy.numWindows, typ)

		for rCopy.HasNext() {
			winIndex, w, err := rCopy.Next()
			if err != nil {
				return nil, err
			}

			var val interface{}
			if !aggr.NeedInclusiveWindow() && w.IsInclusive {
				val, err = aggr.Func()(aggr.InputIndex(), (*w).UnsetInclusive())
			} else {
				val, err = aggr.Func()(aggr.InputIndex(), *w)
			}
			if err != nil {
				return nil, err
			}

			for _, trans := range aggr.Transformations() {
				val, err = trans(val)
				if err != nil {
					return nil, err
				}
			}

			if val == nil {
				continue
			}

			buf.SetOrDrop(winIndex, val)
		}

		if aggr.OutputName() == "" {
			series[colIndex] = bow.NewSeriesFromBuffer(rCopy.bow.ColumnName(aggr.InputIndex()), buf)
		} else {
			series[colIndex] = bow.NewSeriesFromBuffer(aggr.OutputName(), buf)
		}
	}

	return bow.NewBow(series...)
}
