package interpolation

import (
	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
)

func None(colName string) rolling.ColInterpolation {
	return rolling.NewColInterpolation(colName, []bow.Type{bow.Int64, bow.Float64, bow.Bool},
		func(colIndexToFill int, w rolling.Window, fullBow, prevRow bow.Bow) (interface{}, error) {
			return nil, nil
		},
	)
}
