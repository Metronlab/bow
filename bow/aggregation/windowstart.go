package aggregation

import (
	"github.com/Metronlab/bow/bow"
	"github.com/Metronlab/bow/bow/rolling"
)

func WindowStart(col string) rolling.ColumnAggregation {
	return rolling.NewColumnAggregation(col, false, bow.IteratorDependent,
		func(col int, w bow.Window) (interface{}, error) {
			return w.Start, nil
		})
}
