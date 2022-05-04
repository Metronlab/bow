package bow

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToBool(t *testing.T) {
	var v bool
	var ok bool

	v, ok = ToBoolean(true)
	require.True(t, ok)
	assert.Equal(t, true, v)

	v, ok = ToBoolean(false)
	require.True(t, ok)
	assert.Equal(t, false, v)

	v, ok = ToBoolean("true")
	require.True(t, ok)
	assert.Equal(t, true, v)

	v, ok = ToBoolean("True")
	require.True(t, ok)
	assert.Equal(t, true, v)

	v, ok = ToBoolean("false")
	require.True(t, ok)
	assert.Equal(t, false, v)

	v, ok = ToBoolean("False")
	require.True(t, ok)
	assert.Equal(t, false, v)

	v, ok = ToBoolean(1)
	require.True(t, v)
	require.True(t, ok)
	v, ok = ToBoolean(0)
	require.False(t, v)
	require.True(t, ok)

	v, ok = ToBoolean(1.)
	require.True(t, v)
	require.True(t, ok)
	v, ok = ToBoolean(0.)
	require.False(t, v)
	require.True(t, ok)
}

func TestToFloat64(t *testing.T) {
	var v float64
	var ok bool

	v, ok = ToFloat64(true)
	require.True(t, ok)
	assert.Equal(t, 1., v)

	v, ok = ToFloat64(false)
	require.True(t, ok)
	assert.Equal(t, 0., v)

	v, ok = ToFloat64(0.)
	require.True(t, ok)
	assert.Equal(t, 0., v)

	v, ok = ToFloat64(0)
	require.True(t, ok)
	assert.Equal(t, 0., v)

	v, ok = ToFloat64("0")
	require.True(t, ok)
	assert.Equal(t, 0., v)
}

func TestToInt64(t *testing.T) {
	var v int64
	var ok bool

	v, ok = ToInt64(true)
	require.True(t, ok)
	assert.Equal(t, int64(1), v)

	v, ok = ToInt64(false)
	require.True(t, ok)
	assert.Equal(t, int64(0), v)

	v, ok = ToInt64(0.)
	require.True(t, ok)
	assert.Equal(t, int64(0), v)

	v, ok = ToInt64(0)
	require.True(t, ok)
	assert.Equal(t, int64(0), v)

	v, ok = ToInt64("0")
	require.True(t, ok)
	assert.Equal(t, int64(0), v)
}

func TestToString(t *testing.T) {
	var v string
	var ok bool

	v, ok = ToString(true)
	require.True(t, ok)
	assert.Equal(t, "true", v)

	v, ok = ToString(false)
	require.True(t, ok)
	assert.Equal(t, "false", v)

	v, ok = ToString(0.)
	require.True(t, ok)
	assert.Equal(t, "0.000000", v)

	v, ok = ToString(0)
	require.True(t, ok)
	assert.Equal(t, "0", v)

	v, ok = ToString("0")
	require.True(t, ok)
	assert.Equal(t, "0", v)
}

func TestToTimestamp(t *testing.T) {
	t.Run("Sec", func(t *testing.T) {
		v, ok := ToTimestamp(true, arrow.Second)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(1), v)

		v, ok = ToTimestamp(false, arrow.Second)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestamp(0., arrow.Second)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestamp(0, arrow.Second)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestamp("0", arrow.Second)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)
		assert.Equal(t, "1970-01-01T00:00:00Z",
			v.ToTime(arrow.Second).Format(time.RFC3339Nano))

		formattedTimeSec := "2022-04-27T00:00:00Z"
		ti, err := time.Parse(time.RFC3339, formattedTimeSec)
		require.NoError(t, err)

		v, ok = ToTimestamp(ti.Unix(), arrow.Second)
		require.True(t, ok)
		assert.Equal(t, "2022-04-27T00:00:00Z",
			v.ToTime(arrow.Second).Format(time.RFC3339Nano))

		v, ok = ToTimestamp(formattedTimeSec, arrow.Second)
		require.True(t, ok)
		assert.Equal(t, "2022-04-27T00:00:00Z",
			v.ToTime(arrow.Second).Format(time.RFC3339Nano))
	})

	t.Run("Milli", func(t *testing.T) {
		v, ok := ToTimestamp(true, arrow.Millisecond)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(1), v)

		v, ok = ToTimestamp(false, arrow.Millisecond)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestamp(0., arrow.Millisecond)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestamp(0, arrow.Millisecond)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestamp("0", arrow.Millisecond)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)
		assert.Equal(t, "1970-01-01T00:00:00Z",
			v.ToTime(arrow.Millisecond).Format(time.RFC3339Nano))

		formattedTimeMilli := "2022-04-27T00:00:00.123Z"
		ti, err := time.Parse(time.RFC3339, formattedTimeMilli)
		require.NoError(t, err)

		v, ok = ToTimestamp(ti.UnixMilli(), arrow.Millisecond)
		require.True(t, ok)
		assert.Equal(t, "2022-04-27T00:00:00.123Z",
			v.ToTime(arrow.Millisecond).Format(time.RFC3339Nano))

		v, ok = ToTimestamp(formattedTimeMilli, arrow.Millisecond)
		require.True(t, ok)
		assert.Equal(t, "2022-04-27T00:00:00.123Z",
			v.ToTime(arrow.Millisecond).Format(time.RFC3339Nano))
	})

	t.Run("Micro", func(t *testing.T) {
		v, ok := ToTimestamp(true, arrow.Microsecond)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(1), v)

		v, ok = ToTimestamp(false, arrow.Microsecond)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestamp(0., arrow.Microsecond)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestamp(0, arrow.Microsecond)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestamp("0", arrow.Microsecond)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)
		assert.Equal(t, "1970-01-01T00:00:00Z",
			v.ToTime(arrow.Microsecond).Format(time.RFC3339Nano))

		formattedTimeMicro := "2022-04-27T00:00:00.123456Z"
		ti, err := time.Parse(time.RFC3339, formattedTimeMicro)
		require.NoError(t, err)

		v, ok = ToTimestamp(ti.UnixMicro(), arrow.Microsecond)
		require.True(t, ok)
		assert.Equal(t, "2022-04-27T00:00:00.123456Z",
			v.ToTime(arrow.Microsecond).Format(time.RFC3339Nano))

		v, ok = ToTimestamp(formattedTimeMicro, arrow.Microsecond)
		require.True(t, ok)
		assert.Equal(t, "2022-04-27T00:00:00.123456Z",
			v.ToTime(arrow.Microsecond).Format(time.RFC3339Nano))
	})

	t.Run("Nano", func(t *testing.T) {
		v, ok := ToTimestamp(true, arrow.Nanosecond)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(1), v)

		v, ok = ToTimestamp(false, arrow.Nanosecond)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestamp(0., arrow.Nanosecond)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestamp(0, arrow.Nanosecond)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestamp("0", arrow.Nanosecond)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)
		assert.Equal(t, "1970-01-01T00:00:00Z",
			v.ToTime(arrow.Nanosecond).Format(time.RFC3339Nano))

		formattedTimeNano := "2022-04-27T00:00:00.123456789Z"
		ti, err := time.Parse(time.RFC3339, formattedTimeNano)
		require.NoError(t, err)

		v, ok = ToTimestamp(ti.UnixNano(), arrow.Nanosecond)
		require.True(t, ok)
		assert.Equal(t, "2022-04-27T00:00:00.123456789Z",
			v.ToTime(arrow.Nanosecond).Format(time.RFC3339Nano))

		v, ok = ToTimestamp(formattedTimeNano, arrow.Nanosecond)
		require.True(t, ok)
		assert.Equal(t, "2022-04-27T00:00:00.123456789Z",
			v.ToTime(arrow.Nanosecond).Format(time.RFC3339Nano))
	})
}
