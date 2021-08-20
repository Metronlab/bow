package rolling

import "github.com/metronlab/bow"

type Window struct {
	Bow              bow.Bow
	FirstIndex       int // index (across all windows) of first row in this window (-1 if none)
	IntervalColIndex int
	Start            int64
	End              int64
	IsInclusive      bool
}

func (w Window) UnsetInclusive() Window {
	if !w.IsInclusive {
		return w
	}
	cp := w
	cp.IsInclusive = false
	cp.Bow = cp.Bow.NewSlice(0, cp.Bow.NumRows()-1)
	return cp
}
