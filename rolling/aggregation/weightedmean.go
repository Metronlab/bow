package aggregation

import (
	"fmt"
	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
	"time"
)

func timeFromMillisecond(millisecond int64) time.Time {
	return time.Unix(millisecond/1e3, millisecond%1e3*1e6).UTC()
}

func WeightedAverageStep(col string) rolling.ColumnAggregation {
	integralFunc := IntegralStep(col).Func()
	return rolling.NewColumnAggregation(col, false, bow.Float64,
		func(col int, w rolling.Window) (interface{}, error) {
			v, err := integralFunc(col, w)
			if v == nil || err != nil {
				return v, err
			}

			windowsWide := float64(w.End - w.Start)
			fmt.Printf("WeightedAverageStep Col:%d\nWindow Bow:\n%+v\nStart:%s End:%s isInclusive:%v >> RES:%f/%f=%f\n\n",
				col, w.Bow, timeFromMillisecond(w.Start).Format(time.RFC3339), timeFromMillisecond(w.End).Format(time.RFC3339), w.IsInclusive,
				v.(float64), windowsWide, v.(float64)/windowsWide)
			return v.(float64) / windowsWide, nil
		})
}

func WeightedAverageLinear(col string) rolling.ColumnAggregation {
	integralFunc := IntegralTrapezoid(col).Func()
	return rolling.NewColumnAggregation(col, true, bow.Float64,
		func(col int, w rolling.Window) (interface{}, error) {
			v, err := integralFunc(col, w)
			if v == nil || err != nil {
				return v, err
			}

			windowsWide := float64(w.End - w.Start)
			fmt.Printf("WeightedAverageLinear Col:%d\nWindow Bow:\n%+v\nStart:%s End:%s isInclusive:%v >> RES:%f/%f=%f\n\n",
				col, w.Bow, timeFromMillisecond(w.Start).Format(time.RFC3339), timeFromMillisecond(w.End).Format(time.RFC3339), w.IsInclusive,
				v.(float64), windowsWide, v.(float64)/windowsWide)
			return v.(float64) / windowsWide, nil
		})
}
