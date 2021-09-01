package benchmarks

import (
	"fmt"
	"testing"

	"github.com/metronlab/bow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGeneratorForBenchmarks(t *testing.T) {
	//t.Skip("comment this skip to generate new bows for benchmarks")

	for rows := 10; rows <= 100000; rows *= 10 {
		b1, err := bow.NewGenBow(rows,
			bow.GenSeriesOptions{
				Name:        "Int64_ref",
				GenStrategy: bow.GenStrategyRandomIncremental,
			},
			bow.GenSeriesOptions{
				Name:        "Int64_no_nils_bow1",
				GenStrategy: bow.GenStrategyRandom,
			},
			bow.GenSeriesOptions{
				Name:        "Int64_bow1",
				GenStrategy: bow.GenStrategyRandom,
				MissingData: true,
			},
			bow.GenSeriesOptions{
				Name:        "Float64_bow1",
				GenStrategy: bow.GenStrategyRandom,
				MissingData: true,
				Type:        bow.Float64,
			},
			bow.GenSeriesOptions{
				Name:        "Boolean_bow1",
				GenStrategy: bow.GenStrategyRandom,
				MissingData: true,
				Type:        bow.Boolean,
			},
			bow.GenSeriesOptions{
				Name:        "String_bow1",
				GenStrategy: bow.GenStrategyRandom,
				MissingData: true,
				Type:        bow.String,
			},
		)
		require.NoError(t, err)

		b2, err := bow.NewGenBow(rows,
			bow.GenSeriesOptions{
				Name:        "Int64_ref",
				GenStrategy: bow.GenStrategyRandomIncremental,
			},
			bow.GenSeriesOptions{
				Name:        "Int64_bow2",
				GenStrategy: bow.GenStrategyRandom,
				MissingData: true,
			},
			bow.GenSeriesOptions{
				Name:        "Float64_bow2",
				GenStrategy: bow.GenStrategyRandom,
				MissingData: true,
				Type:        bow.Float64,
			},
			bow.GenSeriesOptions{
				Name:        "Boolean_bow2",
				GenStrategy: bow.GenStrategyRandom,
				MissingData: true,
				Type:        bow.Boolean,
			},
			bow.GenSeriesOptions{
				Name:        "String_bow2",
				GenStrategy: bow.GenStrategyRandom,
				MissingData: true,
				Type:        bow.String,
			},
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
