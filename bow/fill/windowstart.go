package fill

import "github.com/Metronlab/bow/bow"
import "github.com/Metronlab/bow/bow/rolling"

func WindowStart(colName string) rolling.ColumnInterpolation {
	return rolling.NewColumnInterpolation(colName, []bow.Type{bow.Int64},
		func(inputCol int, w bow.Window, full bow.Bow) (interface{}, error) {
			return w.Start, nil
		},
	)
}
