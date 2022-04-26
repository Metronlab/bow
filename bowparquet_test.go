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
		bBefore, err := NewBowFromParquet(testInputFileName, true)
		assert.NoError(t, err)
		fmt.Printf("bBefore\n%s\n", bBefore)

		assert.NoError(t, bBefore.WriteParquet(testOutputFileName, true))

		bAfter, err := NewBowFromParquet(testOutputFileName+".parquet", true)
		assert.NoError(t, err)
		fmt.Printf("bAfter\n%s\n", bAfter)

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

		assert.NoError(t, bBefore.WriteParquet(testOutputFileName+"_withrows", true))

		bAfter, err := NewBowFromParquet(testOutputFileName+"_withrows.parquet", true)
		assert.NoError(t, err)

		fmt.Printf("bBefore\n%s\n", bBefore)
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

		assert.NoError(t, bBefore.WriteParquet(testOutputFileName+"_norows", true))

		bAfter, err := NewBowFromParquet(testOutputFileName+"_norows.parquet", true)
		assert.NoError(t, err)

		assert.Equal(t, bBefore.String(), bAfter.String())

		require.NoError(t, os.Remove(testOutputFileName+"_norows.parquet"))
	})

	t.Run("write empty bow", func(t *testing.T) {
		bBefore := NewBowEmpty()

		assert.Errorf(t,
			bBefore.WriteParquet(testOutputFileName+"_empty", true),
			"bow.WriteParquet: no columns",
		)
	})
}
