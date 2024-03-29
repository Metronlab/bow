package aggregation

import (
	"fmt"
	"testing"

	"github.com/metronlab/bow"
	"github.com/stretchr/testify/require"
)

func TestAggregate(t *testing.T) {
	t.Run("empty bow", func(t *testing.T) {
		b, _ := bow.NewBowFromColBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{{}, {}})
		expected, _ := bow.NewBowFromColBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{{}, {}})
		actual, err := Aggregate(b, timeCol,
			WindowStart(timeCol),
			ArithmeticMean(valueCol),
		)
		require.Nil(t, err)
		require.True(t, actual.Equal(expected),
			fmt.Sprintf("expected: %v\nactual: %v", expected, actual))
	})

	t.Run("keep columns", func(t *testing.T) {
		b, _ := bow.NewBowFromColBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10, 20, 30},
				{1., 2., 3.},
			})
		actual, err := Aggregate(b, timeCol,
			WindowStart(timeCol),
			ArithmeticMean(valueCol),
		)
		require.Nil(t, err)
		expected, _ := bow.NewBowFromColBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10},
				{2.},
			})
		require.Nil(t, err)
		require.True(t, actual.Equal(expected),
			fmt.Sprintf("expected: %v\nactual: %v", expected, actual))
	})

	t.Run("swap columns", func(t *testing.T) {
		b, _ := bow.NewBowFromColBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10, 20, 30},
				{1., 2., 3.},
			})
		actual, err := Aggregate(b, timeCol,
			ArithmeticMean(valueCol),
			WindowStart(timeCol),
		)
		require.Nil(t, err)
		expected, _ := bow.NewBowFromColBasedInterfaces(
			[]string{valueCol, timeCol},
			[]bow.Type{bow.Float64, bow.Int64},
			[][]interface{}{
				{2.},
				{10},
			})
		require.Nil(t, err)
		require.True(t, actual.Equal(expected),
			fmt.Sprintf("expected: %v\nactual: %v", expected, actual))
	})

	t.Run("rename columns", func(t *testing.T) {
		b, _ := bow.NewBowFromColBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10, 20, 30},
				{1., 2., 3.},
			})
		actual, err := Aggregate(b, timeCol,
			WindowStart(timeCol).RenameOutput("a"),
			ArithmeticMean(valueCol).RenameOutput("b"),
		)
		require.Nil(t, err)
		expected, _ := bow.NewBowFromColBasedInterfaces(
			[]string{"a", "b"},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10},
				{2.},
			})
		require.Nil(t, err)
		require.True(t, actual.Equal(expected),
			fmt.Sprintf("expected: %v\nactual: %v", expected, actual))
	})

	t.Run("less columns than original", func(t *testing.T) {
		b, _ := bow.NewBowFromColBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10, 20, 30},
				{1., 2., 3.},
			})
		actual, err := Aggregate(b, timeCol,
			ArithmeticMean(valueCol),
		)
		require.Nil(t, err)
		expected, _ := bow.NewBowFromColBasedInterfaces(
			[]string{valueCol},
			[]bow.Type{bow.Float64},
			[][]interface{}{
				{2.},
			})
		require.Nil(t, err)
		require.True(t, actual.Equal(expected),
			fmt.Sprintf("expected: %v\nactual: %v", expected, actual))
	})

	t.Run("more columns than original", func(t *testing.T) {
		b, _ := bow.NewBowFromColBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10, 20, 30},
				{1., 2., 3.},
			})
		actual, err := Aggregate(b, timeCol,
			ArithmeticMean(valueCol).RenameOutput("a"),
			ArithmeticMean(valueCol).RenameOutput("b"),
			ArithmeticMean(valueCol).RenameOutput("c"),
		)
		require.Nil(t, err)
		expected, _ := bow.NewBowFromColBasedInterfaces(
			[]string{"a", "b", "c"},
			[]bow.Type{bow.Float64, bow.Float64, bow.Float64},
			[][]interface{}{
				{2.},
				{2.},
				{2.},
			})
		require.Nil(t, err)
		require.True(t, actual.Equal(expected),
			fmt.Sprintf("expected: %v\nactual: %v", expected, actual))
	})

	t.Run("invalid column", func(t *testing.T) {
		b, _ := bow.NewBowFromColBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10, 20, 30},
				{1., 2., 3.},
			})
		actual, err := Aggregate(b, timeCol,
			WindowStart("-"),
		)
		require.Nil(t, actual)
		require.EqualError(t, err, "column aggregation 0: no column '-'")
	})

	t.Run("float", func(t *testing.T) {
		b, _ := bow.NewBowFromColBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10, 20, 30},
				{1., 2., 3.},
			})
		actual, err := Aggregate(b, timeCol,
			WindowStart(timeCol),
			ArithmeticMean(valueCol),
		)
		require.Nil(t, err)
		expected, _ := bow.NewBowFromColBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10},
				{2.},
			})
		require.Nil(t, err)
		require.True(t, actual.Equal(expected),
			fmt.Sprintf("expected: %v\nactual: %v", expected, actual))
	})
	t.Run("float only nil", func(t *testing.T) {
		b, _ := bow.NewBowFromColBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10, 20, 30},
				{nil, nil, nil},
			})
		actual, err := Aggregate(b, timeCol,
			WindowStart(timeCol),
			WeightedAverageLinear(valueCol),
		)
		require.Nil(t, err)
		expected, _ := bow.NewBowFromColBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10},
				{nil},
			})
		require.Nil(t, err)
		require.True(t, actual.Equal(expected),
			fmt.Sprintf("expected: %v\nactual: %v", expected, actual))
	})
	t.Run("bool", func(t *testing.T) {
		b, _ := bow.NewBowFromColBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Boolean},
			[][]interface{}{
				{10, 20, 30},
				{true, true, false},
			})
		actual, err := Aggregate(b, timeCol,
			WindowStart(timeCol),
			ArithmeticMean(valueCol),
		)
		require.Nil(t, err)
		expected, _ := bow.NewBowFromColBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10},
				{2. / 3.},
			})
		require.Nil(t, err)
		require.True(t, actual.Equal(expected),
			fmt.Sprintf("expected: %v\nactual: %v", expected, actual))
	})
	t.Run("string", func(t *testing.T) {
		b, _ := bow.NewBowFromColBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.String},
			[][]interface{}{
				{10, 20, 30},
				{"1.", "2.", "test"},
			})
		actual, err := Aggregate(b, timeCol,
			WindowStart(timeCol),
			ArithmeticMean(valueCol),
		)
		require.Nil(t, err)
		expected, _ := bow.NewBowFromColBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10},
				{3. / 2.},
			})
		require.Nil(t, err)
		require.True(t, actual.Equal(expected),
			fmt.Sprintf("expected: %v\nactual: %v", expected, actual))
	})
}
