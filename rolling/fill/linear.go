package fill

import (
	"fmt"
	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
)

func Linear(colName string) rolling.ColInterpolation {
	return rolling.NewColInterpolation(colName, []bow.Type{bow.Int64, bow.Float64},
		func(inputCol int, w rolling.Window, fullBow, prevRow bow.Bow) (interface{}, error) {
			var rowIndex = w.FirstIndex
			var err error
			if prevRow != nil {
				fmt.Printf("LINEAR prevRow\n%+v\n", prevRow)
				fullBow, err = bow.AppendBows(prevRow, fullBow)
				if err != nil {
					return nil, err
				}
				rowIndex++
				fmt.Printf("LINEAR rowIndex:%d\n%+v\n", rowIndex, fullBow)
			}

			t0, v0, prevIndex := fullBow.GetPreviousFloat64s(w.IntervalColumnIndex, inputCol, rowIndex-1)
			if prevIndex == -1 {
				return nil, nil
			}
			t2, v2, nextIndex := fullBow.GetNextFloat64s(w.IntervalColumnIndex, inputCol, rowIndex)
			if nextIndex == -1 {
				return nil, nil
			}

			coef := (float64(w.Start) - t0) / (t2 - t0)
			return ((v2 - v0) * coef) + v0, nil
		},
	)
}
