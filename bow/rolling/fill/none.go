package fill

import (
	"github.com/metronlab/bow/bow"
	"github.com/metronlab/bow/bow/rolling"
)

func None(colName string) rolling.ColumnInterpolation {
	return rolling.NewColumnInterpolation(colName, []bow.Type{bow.Int64, bow.Float64, bow.Bool},
		func(inputCol int, w rolling.Window, full bow.Bow) (interface{}, error) {
			return nil, nil
		},
	)
}
