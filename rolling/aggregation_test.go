package rolling

import (
	"fmt"
	"testing"

	"github.com/metronlab/bow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntervalRolling_Aggregate(t *testing.T) {
	b, err := bow.NewBowFromColBasedInterfaces(
		[]string{timeCol, valueCol},
		[]bow.Type{bow.Int64, bow.Float64},
		[][]interface{}{
			{10, 15, 16, 25, 29},
			{1.0, 1.5, 1.6, 2.5, 2.9},
		})
	require.NoError(t, err)
	r, err := IntervalRolling(b, timeCol, 10, Options{})
	require.NoError(t, err)

	timeAggr := NewColAggregation(timeCol, false, bow.Int64,
		func(col int, w Window) (interface{}, error) {
			return w.FirstValue, nil
		})
	valueAggr := NewColAggregation(valueCol, false, bow.Float64,
		func(col int, w Window) (interface{}, error) {
			return float64(w.Bow.NumRows()), nil
		})
	doubleAggr := NewColAggregation(valueCol, false, bow.Float64,
		func(col int, w Window) (interface{}, error) {
			return float64(w.Bow.NumRows()) * 2, nil
		})

	t.Run("keep columns", func(t *testing.T) {
		aggregated, err := r.
			Aggregate(timeAggr, valueAggr).
			Bow()
		assert.NoError(t, err)
		assert.NotNil(t, aggregated)
		expected, _ := bow.NewBowFromColBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10, 20},
				{3., 2.},
			})
		assert.True(t, aggregated.Equal(expected))
	})

	t.Run("swap columns", func(t *testing.T) {
		aggregated, err := r.
			Aggregate(valueAggr, timeAggr).
			Bow()
		assert.NoError(t, err)
		assert.NotNil(t, aggregated)
		expected, _ := bow.NewBowFromColBasedInterfaces(
			[]string{valueCol, timeCol},
			[]bow.Type{bow.Float64, bow.Int64},
			[][]interface{}{
				{3., 2.},
				{10, 20},
			})
		assert.True(t, aggregated.Equal(expected))
	})

	t.Run("rename columns", func(t *testing.T) {
		aggregated, err := r.Aggregate(timeAggr.RenameOutput("a"), valueAggr.RenameOutput("b")).Bow()
		assert.NoError(t, err)
		assert.NotNil(t, aggregated)
		expected, _ := bow.NewBowFromColBasedInterfaces(
			[]string{"a", "b"},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10, 20},
				{3., 2.},
			})
		assert.True(t, aggregated.Equal(expected))
	})

	t.Run("less than in original", func(t *testing.T) {
		aggregated, err := r.Aggregate(timeAggr).Bow()
		assert.NoError(t, err)
		assert.NotNil(t, aggregated)
		expected, _ := bow.NewBowFromColBasedInterfaces(
			[]string{timeCol},
			[]bow.Type{bow.Int64},
			[][]interface{}{
				{10, 20},
			})
		assert.True(t, aggregated.Equal(expected))
	})

	t.Run("more than in original", func(t *testing.T) {
		aggregated, err := r.Aggregate(timeAggr, doubleAggr.RenameOutput("double"), valueAggr).Bow()
		assert.NoError(t, err)
		assert.NotNil(t, aggregated)
		expected, _ := bow.NewBowFromColBasedInterfaces(
			[]string{timeCol, "double", valueCol},
			[]bow.Type{bow.Int64, bow.Float64, bow.Float64},
			[][]interface{}{
				{10, 20},
				{6., 4.},
				{3., 2.},
			})
		assert.True(t, aggregated.Equal(expected))
	})

	t.Run("missing interval colIndex", func(t *testing.T) {
		_, err := r.Aggregate(valueAggr).Bow()
		assert.EqualError(t, err, fmt.Sprintf(
			"intervalRolling.indexedAggregations: must keep interval column '%s'", timeCol))
	})

	t.Run("invalid colIndex", func(t *testing.T) {
		_, err := r.Aggregate(timeAggr, NewColAggregation("-", false, bow.Int64,
			func(col int, w Window) (interface{}, error) { return nil, nil })).Bow()
		assert.EqualError(t, err,
			"intervalRolling.indexedAggregations: no column '-'")
	})
}

func TestWindow_UnsetInclusive(t *testing.T) {
	inclusiveBow, err := bow.NewBowFromColBasedInterfaces(
		[]string{timeCol, valueCol},
		[]bow.Type{bow.Int64, bow.Int64},
		[][]interface{}{
			{1, 2},
			{1, 2}})
	assert.NoError(t, err)
	exclusiveBow, err := bow.NewBowFromColBasedInterfaces(
		[]string{timeCol, valueCol},
		[]bow.Type{bow.Int64, bow.Int64},
		[][]interface{}{
			{1},
			{1}})
	assert.NoError(t, err)

	inclusiveWindow := Window{
		Bow:              inclusiveBow,
		FirstIndex:       0,
		IntervalColIndex: 0,
		FirstValue:       0,
		LastValue:        2,
		IsInclusive:      true,
	}

	exclusiveWindow := inclusiveWindow.UnsetInclusive()
	assert.True(t, exclusiveWindow.Bow.Equal(exclusiveBow))
	exclusiveWindow.Bow = nil
	assert.Equal(t, Window{
		Bow:              nil,
		FirstIndex:       0,
		IntervalColIndex: 0,
		FirstValue:       0,
		LastValue:        2,
		IsInclusive:      false,
	}, exclusiveWindow)

	// inclusive window should not be modified
	assert.Equal(t, Window{
		Bow:              inclusiveBow,
		FirstIndex:       0,
		IntervalColIndex: 0,
		FirstValue:       0,
		LastValue:        2,
		IsInclusive:      true,
	}, inclusiveWindow)
}
