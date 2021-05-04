package aggregation

import (
	"errors"
	"fmt"

	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
)

// Aggregate any column with a ColAggregation
func Aggregate(b bow.Bow, indexColName string, aggrs ...rolling.ColAggregation) (bow.Bow, error) {
	errPrefix := fmt.Sprintf("aggregate on '%s': ", indexColName)

	if b == nil {
		return nil, errors.New(errPrefix + "nil bow")
	}
	if len(aggrs) == 0 {
		return nil, errors.New(errPrefix + "at least one column aggregation is required")
	}

	intervalCol, err := b.GetColIndex(indexColName)
	if err != nil {
		return nil, errors.New(errPrefix + err.Error())
	}

	for i := range aggrs {
		err := validateAggr(b, aggrs[i])
		if err != nil {
			return nil, errors.New(errPrefix + err.Error())
		}
	}

	b2, err := aggregateCols(b, intervalCol, aggrs)
	if err != nil {
		return nil, errors.New(errPrefix + err.Error())
	}

	return b2, nil
}

func validateAggr(b bow.Bow, aggr rolling.ColAggregation) error {
	if aggr.InputName() == "" {
		return fmt.Errorf("no column name")
	}

	readIndex, err := b.GetColIndex(aggr.InputName())
	if err != nil {
		return err
	}

	aggr.MutateInputIndex(readIndex)

	return err
}

func aggregateCols(b bow.Bow, intervalCol int, aggrs []rolling.ColAggregation) (bow.Bow, error) {
	seriess := make([]bow.Series, len(aggrs))

	for writeColIndex, aggr := range aggrs {
		name := aggr.OutputName()
		if name == "" {
			var err error
			name, err = b.GetName(aggr.InputIndex())
			if err != nil {
				return nil, err
			}
		}

		typ := aggr.GetReturnType(b.GetType(aggr.InputIndex()), b.GetType(aggr.InputIndex()))

		if b.IsEmpty() {
			buf := bow.NewBuffer(0, typ, true)
			seriess[writeColIndex] = bow.NewSeries(name, typ, buf.Value, buf.Valid)
			continue
		}

		buf := bow.NewBuffer(1, typ, true)

		firstIndex := -1
		if b.NumRows() > 0 {
			firstIndex = 0
		}
		start, startIndex := b.GetNextFloat64(intervalCol, 0)
		if startIndex == -1 {
			start = -1
		}
		end, endIndex := b.GetPreviousFloat64(intervalCol, b.NumRows()-1)
		if endIndex == -1 {
			end = -1
		}
		w := rolling.Window{
			Bow:                 b,
			IntervalColumnIndex: intervalCol,
			IsInclusive:         true,
			FirstIndex:          firstIndex,
			Start:               int64(start),
			End:                 int64(end),
		}

		val, err := aggr.Func()(aggr.InputIndex(), w)
		if err != nil {
			return nil, err
		}

		for _, transform := range aggr.Transforms() {
			val, err = transform(val)
			if err != nil {
				return nil, err
			}
		}

		buf.SetOrDrop(0, val)
		seriess[writeColIndex] = bow.NewSeries(name, typ, buf.Value, buf.Valid)
	}

	b, err := bow.NewBow(seriess...)
	if err != nil {
		return nil, err
	}

	return b, nil
}
