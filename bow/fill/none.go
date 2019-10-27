package fill

import "github.com/Metronlab/bow/bow"
import "github.com/Metronlab/bow/bow/rolling"

func None(colName string) rolling.ColumnInterpolation {
	return rolling.NewColumnInterpolation(colName, []bow.Type{bow.Int64, bow.Float64, bow.Bool},
		func(inputCol int, w bow.Window, full bow.Bow) (interface{}, error) {
			return nil, nil
		},
	)
}
