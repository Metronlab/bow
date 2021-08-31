package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerator(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		b, err := NewGenBow()
		assert.NoError(t, err)
		assert.Equal(t, genDefaultRows, b.NumRows())
		assert.Equal(t, genDefaultCols, b.NumCols())
		assert.Equal(t, Int64, b.ColumnType(0))

		b2, err := b.DropNils()
		assert.NoError(t, err)
		assert.Equal(t, b, b2)
		assert.True(t, b2.Equal(b), fmt.Sprintf("want %v\ngot %v", b, b2))
	})

	t.Run("with missing data", func(t *testing.T) {
		b, err := NewGenBow(OptionGenMissingData([]int{0, 1, 2}))
		assert.NoError(t, err)
		b2, err := b.DropNils()
		assert.NoError(t, err)
		assert.Less(t, b2.NumRows(), b.NumRows())
	})

	t.Run("float64 with all columns sorted", func(t *testing.T) {
		b, err := NewGenBow(
			OptionGenRows(8),
			OptionGenColTypes([]Type{Float64, Float64}))
		assert.NoError(t, err)

		assert.Equal(t, 8, b.NumRows())
		assert.Equal(t, 2, b.NumCols())
		assert.Equal(t, Float64, b.ColumnType(0))
		assert.Equal(t, Float64, b.ColumnType(1))
		assert.True(t, b.IsColSorted(0))
	})

	t.Run("descending sort on last column", func(t *testing.T) {
		b, err := NewGenBow(
			OptionGenCols(3),
			OptionGenStrategies([]GenStrategy{
				GenStrategyIncremental,
				GenStrategyIncremental,
				GenStrategyDecremental}),
		)
		assert.NoError(t, err)
		sorted := b.IsColSorted(genDefaultCols - 1)
		assert.True(t, sorted)
	})

	t.Run("custom names and types", func(t *testing.T) {
		b, err := NewGenBow(
			OptionGenCols(4),
			OptionGenColNames([]string{"A", "B", "C", "D"}),
			OptionGenColTypes([]Type{Int64, Float64, String, Boolean}),
		)
		assert.NoError(t, err)

		assert.Equal(t, "A", b.ColumnName(0))
		assert.Equal(t, "B", b.ColumnName(1))
		assert.Equal(t, "C", b.ColumnName(2))
		assert.Equal(t, "D", b.ColumnName(3))

		assert.Equal(t, Int64, b.ColumnType(0))
		assert.Equal(t, Float64, b.ColumnType(1))
		assert.Equal(t, String, b.ColumnType(2))
		assert.Equal(t, Boolean, b.ColumnType(3))
	})
}

const (
	benchmarkBowsDirPath = "benchmarks/"
)

func TestGeneratorForBenchmarks(t *testing.T) {

	// comment this line to generate new bows for benchmarks
	t.SkipNow()

	for rows := 10; rows <= 100000; rows *= 10 {
		b1, err := NewGenBow(
			OptionGenCols(6),
			OptionGenRows(rows),
			OptionGenColNames([]string{
				"Int64_ref",
				"Int64_bow1",
				"Int64_no_nils_bow1",
				"Float64_bow1",
				"Boolean_bow1",
				"String_bow1",
			}),
			OptionGenColTypes([]Type{
				Int64,
				Int64,
				Int64,
				Float64,
				Boolean,
				String,
			}),
			OptionGenStrategies([]GenStrategy{
				GenStrategyRandomIncremental,
				GenStrategyRandom,
				GenStrategyRandom,
				GenStrategyRandom,
				GenStrategyRandom,
				GenStrategyRandom,
			}),
			OptionGenMissingData([]int{
				2, 3, 4, 5,
			}),
		)
		require.NoError(t, err)

		b2, err := NewGenBow(
			OptionGenCols(5),
			OptionGenRows(rows),
			OptionGenColNames([]string{
				"Int64_ref",
				"Int64_bow2",
				"Float64_bow2",
				"Boolean_bow2",
				"String_bow2",
			}),
			OptionGenColTypes([]Type{
				Int64,
				Int64,
				Float64,
				Boolean,
				String,
			}),
			OptionGenStrategies([]GenStrategy{
				GenStrategyRandomIncremental,
				GenStrategyRandom,
				GenStrategyRandom,
				GenStrategyRandom,
				GenStrategyRandom,
			}),
			OptionGenMissingData([]int{
				1, 2, 3, 4,
			}),
		)
		require.NoError(t, err)

		assert.NoError(t, b1.WriteParquet(fmt.Sprintf("%sbow1-%d-rows", benchmarkBowsDirPath, rows), false))
		_, err = NewBowFromParquet(fmt.Sprintf("%sbow1-%d-rows.parquet", benchmarkBowsDirPath, rows), false)
		assert.NoError(t, err)

		assert.NoError(t, b2.WriteParquet(fmt.Sprintf("%sbow2-%d-rows", benchmarkBowsDirPath, rows), false))
		_, err = NewBowFromParquet(fmt.Sprintf("%sbow2-%d-rows.parquet", benchmarkBowsDirPath, rows), false)
		assert.NoError(t, err)
	}
}
