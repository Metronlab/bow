package aggregation

import (
	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"
)

func WeightedAverageStep(col string) rolling.ColumnAggregation {
	integralFunc := IntegralStep(col).Func()
	return rolling.NewColumnAggregation(col, false, bow.Float64,
		func(col int, w bow.Window) (interface{}, error) {
			v, err := integralFunc(col, w)
			if v == nil || err != nil {
				return v, err
			}

			return v.(float64) / float64(w.End-w.Start), nil
		})
}

func WeightedAverageLinear(col string) rolling.ColumnAggregation {
	integralFunc := IntegralTrapezoid(col).Func()
	return rolling.NewColumnAggregation(col, true, bow.Float64,
		func(col int, w bow.Window) (interface{}, error) {
			v, err := integralFunc(col, w)
			if v == nil || err != nil {
				return v, err
			}

			windowsWide := float64(w.End - w.Start)
			return v.(float64) / windowsWide, nil
		})
}
