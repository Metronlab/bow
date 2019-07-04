package bow

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWindow_UnsetInclusive(t *testing.T) {
	inclusiveBow, err := NewBowFromColumnBasedInterfaces([]string{"time", "value"}, []Type{Int64, Int64},
		[][]interface{}{
			{1, 2},
			{1, 2}})
	assert.NoError(t, err)
	exclusiveBow, err := NewBowFromColumnBasedInterfaces([]string{"time", "value"}, []Type{Int64, Int64},
		[][]interface{}{
			{1},
			{1}})
	assert.NoError(t, err)

	inclusiveWindow := Window{
		Bow:                 inclusiveBow,
		FirstIndex:          0,
		IntervalColumnIndex: 0,
		Start:               0,
		End:                 2,
		IsInclusive:         true,
	}

	exclusiveWindow := inclusiveWindow.UnsetInclusive()
	assert.True(t, exclusiveWindow.Bow.Equal(exclusiveBow))
	exclusiveWindow.Bow = nil
	assert.Equal(t, Window{
		Bow:                 nil,
		FirstIndex:          0,
		IntervalColumnIndex: 0,
		Start:               0,
		End:                 2,
		IsInclusive:         false,
	}, exclusiveWindow)

	// inclusive window should not be modified
	assert.Equal(t, Window{
		Bow:                 inclusiveBow,
		FirstIndex:          0,
		IntervalColumnIndex: 0,
		Start:               0,
		End:                 2,
		IsInclusive:         true,
	}, inclusiveWindow)
}
