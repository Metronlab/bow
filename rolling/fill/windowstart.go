package fill

import (
	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
)

func WindowStart(colName string) rolling.ColumnInterpolation {
	return rolling.NewColumnInterpolation(colName, []bow.Type{bow.Int64},
		func(inputCol int, w rolling.Window, full bow.Bow) (interface{}, error) {
			return w.Start, nil
		},
	)
}
