package interpolation

import (
	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
)

func WindowStart(colName string) rolling.ColInterpolation {
	return rolling.NewColInterpolation(colName, []bow.Type{bow.Int64},
		func(colIndexToFill int, w rolling.Window, fullBow, prevRow bow.Bow) (interface{}, error) {
			return w.FirstValue, nil
		},
	)
}
