package aggregation

import (
	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
)

func WindowStart(col string) rolling.ColumnAggregation {
	return rolling.NewColumnAggregation(col, false, bow.IteratorDependent,
		func(col int, w rolling.Window) (interface{}, error) {
			return w.Start, nil
		})
}
