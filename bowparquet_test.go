package bow

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testInputFileName  = "bowparquet_test_input.parquet"
	testOutputFileName = "/tmp/bowparquet_test_output"
)

func TestParquet(t *testing.T) {
	t.Run("read/write input file", func(t *testing.T) {
		bBefore, err := NewBowFromParquet(testInputFileName, false)
		assert.NoError(t, err)

		assert.NoError(t, bBefore.WriteParquet(testOutputFileName, false))

		bAfter, err := NewBowFromParquet(testOutputFileName+".parquet", false)
		assert.NoError(t, err)

		assert.Equal(t, bBefore.String(), bAfter.String())

		require.NoError(t, os.Remove(testOutputFileName+".parquet"))
	})

	t.Run("bow supported types with rows and nils", func(t *testing.T) {
		bBefore, err := NewBowFromRowBasedInterfaces(
			[]string{"int", "float", "bool", "string"},
			[]Type{Int64, Float64, Bool, String},
			[][]interface{}{
				{1, 1., true, "hi"},
				{2, 2., false, "ho"},
				{nil, nil, nil, nil},
				{3, 3., true, "hu"},
			})
		require.NoError(t, err)

		assert.NoError(t, bBefore.WriteParquet(testOutputFileName+"_withrows", false))

		bAfter, err := NewBowFromParquet(testOutputFileName+"_withrows.parquet", false)
		assert.NoError(t, err)

		assert.Equal(t, true, bBefore.Schema().Equal(bAfter.Schema()))
		assert.Equal(t, bBefore.NumRows(), bAfter.NumRows())

		require.NoError(t, os.Remove(testOutputFileName+"_withrows.parquet"))
	})

	t.Run("bow supported types without rows", func(t *testing.T) {
		bBefore, err := NewBowFromRowBasedInterfaces(
			[]string{"int", "float", "bool", "string"},
			[]Type{Int64, Float64, Bool, String},
			[][]interface{}{})
		require.NoError(t, err)

		assert.NoError(t, bBefore.WriteParquet(testOutputFileName+"_norows", false))

		bAfter, err := NewBowFromParquet(testOutputFileName+"_norows.parquet", false)
		assert.NoError(t, err)

		assert.Equal(t, true, bBefore.Schema().Equal(bAfter.Schema()))
		assert.Equal(t, bBefore.NumRows(), bAfter.NumRows())

		require.NoError(t, os.Remove(testOutputFileName+"_norows.parquet"))
	})

	t.Run("write empty bow", func(t *testing.T) {
		bBefore := NewBowEmpty()

		err := bBefore.WriteParquet(testOutputFileName+"_empty", false)
		assert.Errorf(t, err, "bow.WriteParquet: no columns")
	})

	t.Run("bow with context and col_types metadata", func(t *testing.T) {
		var series = make([]Series, 2)

		series[0] = NewSeries("time", Int64, []int64{0}, []bool{true})
		series[1] = NewSeries("value", Float64, []float64{0.}, []bool{true})

		var keys, values []string
		type Unit struct {
			Symbol string `json:"symbol"`
		}
		type Meta struct {
			Unit Unit `json:"unit"`
		}
		type Context map[string]Meta

		var context = Context{
			"time":  Meta{Unit{Symbol: "microseconds"}},
			"value": Meta{Unit{Symbol: "kWh"}},
		}

		contextJSON, err := json.Marshal(context)
		assert.NoError(t, err)

		keys = append(keys, "context")
		values = append(values, string(contextJSON))

		bBefore, err := NewBowWithMetadata(
			NewMetaWithParquetTimestampMicrosCols(keys, values, "time"),
			series...)
		assert.NoError(t, err)

		err = bBefore.WriteParquet(testOutputFileName+"_meta", false)
		assert.NoError(t, err)

		bAfter, err := NewBowFromParquet(testOutputFileName+"_meta.parquet", false)
		assert.NoError(t, err)

		assert.Equal(t, bBefore.String(), bAfter.String())

		require.NoError(t, os.Remove(testOutputFileName+"_meta.parquet"))
	})

	t.Run("bow with wrong col_types metadata", func(t *testing.T) {
		var series = make([]Series, 2)

		series[0] = NewSeries("time", Int64, []int64{0}, []bool{true})
		series[1] = NewSeries("value", Float64, []float64{0.}, []bool{true})

		var keys, values []string

		bBefore, err := NewBowWithMetadata(
			NewMetaWithParquetTimestampMicrosCols(keys, values, "unknown"),
			series...)
		assert.NoError(t, err)

		err = bBefore.WriteParquet(testOutputFileName+"_wrong", false)
		assert.Error(t, err)
	})
}
