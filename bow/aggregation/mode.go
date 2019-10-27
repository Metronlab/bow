package aggregation

import (
	"github.com/metronlab/bow/bow"
	"github.com/metronlab/bow/bow/rolling"
)

func Mode(col string) rolling.ColumnAggregation {
	return rolling.NewColumnAggregation(col, false, bow.InputDependent,
		func(col int, w bow.Window) (interface{}, error) {
			if w.Bow.NumRows() == 0 {
				return nil, nil
			}

			occurrences := make(map[interface{}]int)
			max := 0
			var res interface{}
			for i := 0; i < w.Bow.NumRows(); i++ {
				v := w.Bow.GetValue(col, i)
				if v != nil {
					nb := occurrences[v]
					nb++
					occurrences[v] = nb
					if nb > max {
						max = nb
						res = v
					}
				}
			}
			return res, nil
		})
}
