package aggregation

import (
	"fmt"

	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
)

// Aggregate any column with a ColAggregation
func Aggregate(b bow.Bow, refColName string, aggrs ...rolling.ColAggregation) (bow.Bow, error) {
	if b == nil {
		return nil, fmt.Errorf("aggregate on '%s': nil bow", refColName)
	}
	if len(aggrs) == 0 {
		return nil, fmt.Errorf("aggregate on '%s': at least one column aggregation is required", refColName)
	}

	for i := range aggrs {
		err := validateAggr(b, aggrs[i])
		if err != nil {
			return nil, fmt.Errorf("aggregate on '%s': %w", refColName, err)
		}
	}

	refColIndex, err := b.GetColIndex(refColName)
	if err != nil {
		return nil, err
	}

	aggregatedBow, err := aggregateCols(b, refColIndex, aggrs)
	if err != nil {
		return nil, fmt.Errorf("aggregate on '%s': %w", refColName, err)
	}

	return aggregatedBow, nil
}

func validateAggr(b bow.Bow, aggr rolling.ColAggregation) error {
	if aggr.InputName() == "" {
		return fmt.Errorf("no column name")
	}

	colIndex, err := b.GetColIndex(aggr.InputName())
	if err != nil {
		return err
	}

	aggr.MutateInputIndex(colIndex)

	return nil
}

// TODO: optimize this function with concurrency and less memory usage for accessing intervalCol data
func aggregateCols(b bow.Bow, refColIndex int, aggrs []rolling.ColAggregation) (bow.Bow, error) {
	seriesSlice := make([]bow.Series, len(aggrs))

	for writeColIndex, aggr := range aggrs {
		name := aggr.OutputName()
		if name == "" {
			name = b.GetColName(aggr.InputIndex())
		}

		typ := aggr.GetReturnType(
			b.GetColType(aggr.InputIndex()),
			b.GetColType(aggr.InputIndex()))

		if b.IsEmpty() {
			buf := bow.NewBuffer(0, typ, true)
			seriesSlice[writeColIndex] = bow.NewSeries(name, typ, buf.Value, buf.Valid)
			continue
		}

		buf := bow.NewBuffer(1, typ, true)

		firstIndex := -1
		if b.NumRows() > 0 {
			firstIndex = 0
		}
		start, startIndex := b.GetNextFloat64(refColIndex, 0)
		if startIndex == -1 {
			start = -1
		}
		end, endIndex := b.GetPreviousFloat64(refColIndex, b.NumRows()-1)
		if endIndex == -1 {
			end = -1
		}
		w := rolling.Window{
			Bow:              b,
			IntervalColIndex: refColIndex,
			IsInclusive:      true,
			FirstIndex:       firstIndex,
			Start:            int64(start),
			End:              int64(end),
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
		seriesSlice[writeColIndex] = bow.NewSeries(name, typ, buf.Value, buf.Valid)
	}

	return bow.NewBow(seriesSlice...)
}
