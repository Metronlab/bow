package benchmarks

import (
	"fmt"
	"github.com/metronlab/bow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGeneratorForBenchmarks(t *testing.T) {
	t.Skip("comment this skip to generate new bows for benchmarks")

	for rows := 10; rows <= 100000; rows *= 10 {
		b1, err := bow.NewGenBow(
			bow.OptionGenCols(6),
			bow.OptionGenRows(rows),
			bow.OptionGenColNames([]string{
				"Int64_ref",
				"Int64_bow1",
				"Int64_no_nils_bow1",
				"Float64_bow1",
				"Boolean_bow1",
				"String_bow1",
			}),
			bow.OptionGenColTypes([]bow.Type{
				bow.Int64,
				bow.Int64,
				bow.Int64,
				bow.Float64,
				bow.Boolean,
				bow.String,
			}),
			bow.OptionGenStrategies([]bow.GenStrategy{
				bow.GenStrategyRandomIncremental,
				bow.GenStrategyRandom,
				bow.GenStrategyRandom,
				bow.GenStrategyRandom,
				bow.GenStrategyRandom,
				bow.GenStrategyRandom,
			}),
			bow.OptionGenMissingData([]int{
				2, 3, 4, 5,
			}),
		)
		require.NoError(t, err)

		b2, err := bow.NewGenBow(
			bow.OptionGenCols(5),
			bow.OptionGenRows(rows),
			bow.OptionGenColNames([]string{
				"Int64_ref",
				"Int64_bow2",
				"Float64_bow2",
				"Boolean_bow2",
				"String_bow2",
			}),
			bow.OptionGenColTypes([]bow.Type{
				bow.Int64,
				bow.Int64,
				bow.Float64,
				bow.Boolean,
				bow.String,
			}),
			bow.OptionGenStrategies([]bow.GenStrategy{
				bow.GenStrategyRandomIncremental,
				bow.GenStrategyRandom,
				bow.GenStrategyRandom,
				bow.GenStrategyRandom,
				bow.GenStrategyRandom,
			}),
			bow.OptionGenMissingData([]int{
				1, 2, 3, 4,
			}),
		)
		require.NoError(t, err)

		assert.NoError(t, b1.WriteParquet(fmt.Sprintf("./bow1-%d-rows", rows), false))
		_, err = bow.NewBowFromParquet(fmt.Sprintf("./bow1-%d-rows.parquet", rows), false)
		assert.NoError(t, err)

		assert.NoError(t, b2.WriteParquet(fmt.Sprintf("./bow2-%d-rows", rows), false))
		_, err = bow.NewBowFromParquet(fmt.Sprintf("./bow2-%d-rows.parquet", rows), false)
		assert.NoError(t, err)
	}
}
