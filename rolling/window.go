package rolling

import "github.com/metronlab/bow"

// Window represents an interval-based window of data with:
// Bow: data
// FirstIndex: index (across all windows) of first row in this window (-1 if none)
// IntervalColIndex: index of the interval column
// FirstValue: Window first value
// LastValue: Window last value
// IsInclusive: Window is inclusive, i.e. includes the last point at the end of the interval
type Window struct {
	Bow              bow.Bow
	FirstIndex       int
	IntervalColIndex int
	FirstValue       int64
	LastValue        int64
	IsInclusive      bool
}

// UnsetInclusive returns a copy of the Window with the IsInclusive parameter set to false and with the last row sliced off.
// Returns the unchanged Window if the IsInclusive parameter is not set.
func (w Window) UnsetInclusive() Window {
	if !w.IsInclusive {
		return w
	}
	wCopy := w
	wCopy.IsInclusive = false
	wCopy.Bow = wCopy.Bow.NewSlice(0, wCopy.Bow.NumRows()-1)
	return wCopy
}
