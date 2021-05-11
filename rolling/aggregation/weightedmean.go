package aggregation

import (
	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
)

func WeightedAverageStep(col string) rolling.ColumnAggregation {
	integralFunc := IntegralStep(col).Func()
	return rolling.NewColumnAggregation(col, false, bow.Float64,
		func(colIndex int, w rolling.Window) (interface{}, error) {
			v, err := integralFunc(colIndex, w)
			if v == nil || err != nil {
				return v, err
			}

			windowsWide := float64(w.End - w.Start)
			return v.(float64) / windowsWide, nil
		})
}

func WeightedAverageLinear(col string) rolling.ColumnAggregation {
	integralFunc := IntegralTrapezoid(col).Func()
	return rolling.NewColumnAggregation(col, true, bow.Float64,
		func(colIndex int, w rolling.Window) (interface{}, error) {
			v, err := integralFunc(colIndex, w)
			if v == nil || err != nil {
				return v, err
			}

			windowsWide := float64(w.End - w.Start)
			return v.(float64) / windowsWide, nil
		})
}
