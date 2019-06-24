package aggregation

import (
	"git.metronlab.com/backend_libraries/go-bow/bow"
	"git.metronlab.com/backend_libraries/go-bow/bow/intepolation"
	"git.metronlab.com/backend_libraries/go-bow/bow/rolling"
)

func WeightedMean(col string, interpol intepolation.Interpolation) rolling.ColumnAggregation {
	integralFunc := Integral(col, interpol).Func()
	return rolling.NewColumnAggregation(col, bow.Float64,
		func(col int, w bow.Window) (interface{}, error) {
			v, err := integralFunc(col, w)
			if v == nil || err != nil {
				return v, err
			}

			windowsWide := float64(w.End - w.Start)
			return v.(float64) / windowsWide, nil
		})
}
