package bow

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testInputFileName  = "bowparquet_test_input.parquet"
	testOutputFileName = "bowparquet_test_output"
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

	t.Run("bow supported types with rows", func(t *testing.T) {
		bBefore, err := NewBowFromRowBasedInterfaces(
			[]string{"int", "float", "bool", "string"},
			[]Type{Int64, Float64, Bool, String},
			[][]interface{}{
				{1, 1., true, "hi"},
				{2, 2., false, "ho"},
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
}