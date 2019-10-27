package aggregation

import (
	"github.com/metronlab/bow/bow"
	"github.com/metronlab/bow/bow/rolling"
)

func WindowStart(col string) rolling.ColumnAggregation {
	return rolling.NewColumnAggregation(col, false, bow.IteratorDependent,
		func(col int, w bow.Window) (interface{}, error) {
			return w.Start, nil
		})
}
