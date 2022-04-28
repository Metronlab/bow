package bow

import (
	"fmt"
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

	t.Run("all supported types with rows and nil values", func(t *testing.T) {
		bBefore, err := NewBowFromRowBasedInterfaces(
			[]string{"int", "float", "bool", "string",
				"timestamp_ms_int", "timestamp_ms_str",
				"timestamp_us_int", "timestamp_us_str",
				"timestamp_ns_int", "timestamp_ns_str"},
			[]Type{Int64, Float64, Bool, String,
				TimestampMilli, TimestampMilli,
				TimestampMicro, TimestampMicro,
				TimestampNano, TimestampNano},
			[][]interface{}{
				{1, 1., true, "hi",
					1651017600000, "2022-04-27T00:00:00.123Z",
					1651017600000000, "2022-04-27T00:00:00.123456Z",
					1651017600000000000, "2022-04-27T00:00:00.123456789Z"},
				{2, 2., false, "ho",
					1651021200000, "2022-04-27T01:00:00.123Z",
					1651021200000000, "2022-04-27T01:00:00.123456Z",
					1651021200000000000, "2022-04-27T01:00:00.123456789Z"},
				{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
				{3, 3., true, "hu",
					1651028400000, "2022-04-27T03:00:00.123Z",
					1651028400000000, "2022-04-27T03:00:00.123Z456",
					1651028400000000000, "2022-04-27T03:00:00.123456789Z"},
			})
		require.NoError(t, err)

		fmt.Printf("bBefore\n%s\n", bBefore)

		assert.NoError(t, bBefore.WriteParquet(testOutputFileName+"_withrows", true))

		bAfter, err := NewBowFromParquet(testOutputFileName+"_withrows.parquet", true)
		assert.NoError(t, err)

		fmt.Printf("bAfter\n%s\n", bAfter)

		assert.Equal(t, bBefore.String(), bAfter.String())

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

		assert.Equal(t, bBefore.String(), bAfter.String())

		require.NoError(t, os.Remove(testOutputFileName+"_norows.parquet"))
	})

	t.Run("write empty bow", func(t *testing.T) {
		bBefore := NewBowEmpty()

		assert.Errorf(t,
			bBefore.WriteParquet(testOutputFileName+"_empty", false),
			"bow.WriteParquet: no columns",
		)
	})
}
