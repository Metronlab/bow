package bow

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go/parquet"
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
			[]string{"int", "float", "bool", "string"},
			[]Type{Int64, Float64, Boolean, String},
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

		assert.Equal(t, bBefore.String(), bAfter.String())

		require.NoError(t, os.Remove(testOutputFileName+"_withrows.parquet"))
	})

	t.Run("bow supported types without rows", func(t *testing.T) {
		bBefore, err := NewBowFromRowBasedInterfaces(
			[]string{"int", "float", "bool", "string"},
			[]Type{Int64, Float64, Boolean, String},
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

	t.Run("bow with context and col_types metadata", func(t *testing.T) {
		var series = make([]Series, 2)
		series[0] = NewSeries("time", []int64{0}, []bool{true})
		series[1] = NewSeries("  va\"lue  ", []float64{0.}, []bool{true})

		var keys, values []string
		type Unit struct {
			Symbol string `json:"symbol"`
		}
		type Meta struct {
			Unit Unit `json:"unit"`
		}
		type Context map[string]Meta

		var ctx = Context{
			"time":        Meta{Unit{Symbol: "microseconds"}},
			"  va\"lue  ": Meta{Unit{Symbol: "kWh"}},
		}

		contextJSON, err := json.Marshal(ctx)
		require.NoError(t, err)

		keys = append(keys, "context")
		values = append(values, string(contextJSON))

		bBefore, err := NewBowWithMetadata(
			newMetaWithParquetTimestampCol(keys, values, "time", time.Microsecond),
			series...)
		require.NoError(t, err)

		err = bBefore.WriteParquet(testOutputFileName+"_meta", false)
		assert.NoError(t, err)

		bAfter, err := NewBowFromParquet(testOutputFileName+"_meta.parquet", false)
		assert.NoError(t, err)

		assert.Equal(t, bBefore.String(), bAfter.String())

		require.NoError(t, os.Remove(testOutputFileName+"_meta.parquet"))
	})

	t.Run("bow with wrong col_types metadata", func(t *testing.T) {
		var series = make([]Series, 2)

		series[0] = NewSeries("time", []int64{0}, []bool{true})
		series[1] = NewSeries("value", []float64{0.}, []bool{true})

		var keys, values []string

		bBefore, err := NewBowWithMetadata(
			newMetaWithParquetTimestampCol(keys, values, "unknown", time.Microsecond),
			series...)
		assert.NoError(t, err)

		assert.Error(t, bBefore.WriteParquet(testOutputFileName+"_wrong", false))
	})
}

func TestBowGetParquetMetaColTimeUnit(t *testing.T) {
	timeCol := "time"
	var series = make([]Series, 2)
	series[0] = NewSeries(timeCol, []int64{0}, nil)
	series[1] = NewSeries("value", []float64{0.}, nil)

	t.Run("time.Millisecond", func(t *testing.T) {
		b, err := NewBowWithMetadata(
			newMetaWithParquetTimestampCol([]string{}, []string{}, timeCol, time.Millisecond),
			series...)
		require.NoError(t, err)

		got, err := b.GetParquetMetaColTimeUnit(0)
		require.NoError(t, err)
		assert.Equal(t, time.Millisecond, got)
	})

	t.Run("time.Microsecond", func(t *testing.T) {
		b, err := NewBowWithMetadata(
			newMetaWithParquetTimestampCol([]string{}, []string{}, timeCol, time.Microsecond),
			series...)
		require.NoError(t, err)

		got, err := b.GetParquetMetaColTimeUnit(0)
		require.NoError(t, err)
		assert.Equal(t, time.Microsecond, got)
	})

	t.Run("time.Nanosecond", func(t *testing.T) {
		b, err := NewBowWithMetadata(
			newMetaWithParquetTimestampCol([]string{}, []string{}, timeCol, time.Nanosecond),
			series...)
		require.NoError(t, err)

		got, err := b.GetParquetMetaColTimeUnit(0)
		require.NoError(t, err)
		assert.Equal(t, time.Nanosecond, got)
	})

	t.Run("column without timestamp metadata", func(t *testing.T) {
		b, err := NewBowWithMetadata(
			newMetaWithParquetTimestampCol([]string{}, []string{}, timeCol, time.Nanosecond),
			series...)
		require.NoError(t, err)

		got, err := b.GetParquetMetaColTimeUnit(1)
		require.ErrorIs(t, err, ErrColTimeUnitNotFound)
		require.Equal(t, time.Duration(0), got)
	})

	t.Run("column out of range", func(t *testing.T) {
		b, err := NewBowWithMetadata(
			newMetaWithParquetTimestampCol([]string{}, []string{}, timeCol, time.Nanosecond),
			series...)
		require.NoError(t, err)

		assert.Panics(t, func() {
			_, _ = b.GetParquetMetaColTimeUnit(42)
		})
	})
}

func newMetaWithParquetTimestampCol(keys, values []string, colName string, timeUnit time.Duration) Metadata {
	var colTypes = make([]parquetColTypesMeta, 1)

	unit := parquet.TimeUnit{}
	switch timeUnit {
	case time.Millisecond:
		unit.MILLIS = &parquet.MilliSeconds{}
	case time.Microsecond:
		unit.MICROS = &parquet.MicroSeconds{}
	case time.Nanosecond:
		unit.NANOS = &parquet.NanoSeconds{}
	default:
		panic(fmt.Errorf("unsupported time unit '%s'", timeUnit))
	}

	logicalType := parquet.LogicalType{
		TIMESTAMP: &parquet.TimestampType{
			IsAdjustedToUTC: true,
			Unit:            &unit,
		}}
	colTypes[0] = parquetColTypesMeta{
		Name:        colName,
		LogicalType: &logicalType,
	}

	colTypesJSON, err := json.Marshal(colTypes)
	if err != nil {
		panic(err)
	}

	keys = append(keys, keyParquetColTypesMeta)
	values = append(values, string(colTypesJSON))

	return Metadata{arrow.NewMetadata(keys, values)}
}
