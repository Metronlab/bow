package bow

type Window struct {
	Bow                 Bow
	IntervalColumnIndex int
	Start               float64
	End                 float64
	IsInclusive         bool
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
