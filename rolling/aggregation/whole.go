package aggregation

import (
	"errors"
	"fmt"

	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
)

// Aggregate the whole dataframe on column intervalColName with one or several rolling.ColAggregation.
func Aggregate(b bow.Bow, intervalColName string, aggrs ...rolling.ColAggregation) (bow.Bow, error) {
	if b == nil {
		return nil, errors.New("nil bow")
	}
	if len(aggrs) == 0 {
		return nil, errors.New("at least one column aggregation is required")
	}

	intervalColIndex, err := b.ColumnIndex(intervalColName)
	if err != nil {
		return nil, err
	}

	series := make([]bow.Series, len(aggrs))

	for aggrIndex, aggr := range aggrs {
		if aggr.InputName() == "" {
			return nil, fmt.Errorf("column aggregation %d: no input name", aggrIndex)
		}

		inputColIndex, err := b.ColumnIndex(aggr.InputName())
		if err != nil {
			return nil, fmt.Errorf("column aggregation %d: %w", aggrIndex, err)
		}

		aggr.SetInputIndex(inputColIndex)

		name := aggr.OutputName()
		if name == "" {
			name = b.ColumnName(aggr.InputIndex())
		}

		typ := aggr.GetReturnType(
			b.ColumnType(aggr.InputIndex()),
			b.ColumnType(aggr.InputIndex()))

		var buf bow.Buffer
		if b.NumRows() == 0 {
			buf = bow.NewBuffer(0, typ)
		} else {
			buf = bow.NewBuffer(1, typ)

			firstValue, firstValueIndex := b.GetNextFloat64(intervalColIndex, 0)
			if firstValueIndex == -1 {
				firstValue = -1
			}

			lastValue, lastValueIndex := b.GetPrevFloat64(intervalColIndex, b.NumRows()-1)
			if lastValueIndex == -1 {
				lastValue = -1
			}

			w := rolling.Window{
				Bow:              b,
				IntervalColIndex: intervalColIndex,
				IsInclusive:      true,
				FirstIndex:       0,
				FirstValue:       int64(firstValue),
				LastValue:        int64(lastValue),
			}

			aggrValue, err := aggr.Func()(aggr.InputIndex(), w)
			if err != nil {
				return nil, fmt.Errorf("column aggregation %d: %w", aggrIndex, err)
			}

			for transIndex, trans := range aggr.Transformations() {
				aggrValue, err = trans(aggrValue)
				if err != nil {
					return nil, fmt.Errorf("column aggregation %d: transIndex %d: %w",
						aggrIndex, transIndex, err)
				}
			}

			buf.SetOrDropStrict(0, aggrValue)
		}

		series[aggrIndex] = bow.NewSeriesFromBuffer(name, buf)
	}

	return bow.NewBow(series...)
}
