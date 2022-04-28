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
		v, ok := ToTimestampSec(true)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(1), v)

		v, ok = ToTimestampSec(false)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestampSec(0.)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestampSec(0)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestampSec("0")
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)
		assert.Equal(t, "1970-01-01T00:00:00Z",
			v.ToTime(arrow.Second).Format(time.RFC3339Nano))

		formattedTimeSec := "2022-04-27T00:00:00Z"
		ti, err := time.Parse(time.RFC3339, formattedTimeSec)
		require.NoError(t, err)

		v, ok = ToTimestampSec(ti.Unix())
		require.True(t, ok)
		assert.Equal(t, "2022-04-27T00:00:00Z",
			v.ToTime(arrow.Second).Format(time.RFC3339Nano))

		v, ok = ToTimestampSec(formattedTimeSec)
		require.True(t, ok)
		assert.Equal(t, "2022-04-27T00:00:00Z",
			v.ToTime(arrow.Second).Format(time.RFC3339Nano))
	})

	t.Run("Milli", func(t *testing.T) {
		v, ok := ToTimestampMilli(true)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(1), v)

		v, ok = ToTimestampMilli(false)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestampMilli(0.)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestampMilli(0)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestampMilli("0")
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)
		assert.Equal(t, "1970-01-01T00:00:00Z",
			v.ToTime(arrow.Millisecond).Format(time.RFC3339Nano))

		formattedTimeMilli := "2022-04-27T00:00:00.123Z"
		ti, err := time.Parse(time.RFC3339, formattedTimeMilli)
		require.NoError(t, err)

		v, ok = ToTimestampMilli(ti.UnixMilli())
		require.True(t, ok)
		assert.Equal(t, "2022-04-27T00:00:00.123Z",
			v.ToTime(arrow.Millisecond).Format(time.RFC3339Nano))

		v, ok = ToTimestampMilli(formattedTimeMilli)
		require.True(t, ok)
		assert.Equal(t, "2022-04-27T00:00:00.123Z",
			v.ToTime(arrow.Millisecond).Format(time.RFC3339Nano))
	})

	t.Run("Micro", func(t *testing.T) {
		v, ok := ToTimestampMicro(true)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(1), v)

		v, ok = ToTimestampMicro(false)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestampMicro(0.)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestampMicro(0)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestampMicro("0")
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)
		assert.Equal(t, "1970-01-01T00:00:00Z",
			v.ToTime(arrow.Microsecond).Format(time.RFC3339Nano))

		formattedTimeMicro := "2022-04-27T00:00:00.123456Z"
		ti, err := time.Parse(time.RFC3339, formattedTimeMicro)
		require.NoError(t, err)

		v, ok = ToTimestampMicro(ti.UnixMicro())
		require.True(t, ok)
		assert.Equal(t, "2022-04-27T00:00:00.123456Z",
			v.ToTime(arrow.Microsecond).Format(time.RFC3339Nano))

		v, ok = ToTimestampMicro(formattedTimeMicro)
		require.True(t, ok)
		assert.Equal(t, "2022-04-27T00:00:00.123456Z",
			v.ToTime(arrow.Microsecond).Format(time.RFC3339Nano))
	})

	t.Run("Nano", func(t *testing.T) {
		v, ok := ToTimestampNano(true)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(1), v)

		v, ok = ToTimestampNano(false)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestampNano(0.)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestampNano(0)
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)

		v, ok = ToTimestampNano("0")
		require.True(t, ok)
		assert.Equal(t, arrow.Timestamp(0), v)
		assert.Equal(t, "1970-01-01T00:00:00Z",
			v.ToTime(arrow.Nanosecond).Format(time.RFC3339Nano))

		formattedTimeNano := "2022-04-27T00:00:00.123456789Z"
		ti, err := time.Parse(time.RFC3339, formattedTimeNano)
		require.NoError(t, err)

		v, ok = ToTimestampNano(ti.UnixNano())
		require.True(t, ok)
		assert.Equal(t, "2022-04-27T00:00:00.123456789Z",
			v.ToTime(arrow.Nanosecond).Format(time.RFC3339Nano))

		v, ok = ToTimestampNano(formattedTimeNano)
		require.True(t, ok)
		assert.Equal(t, "2022-04-27T00:00:00.123456789Z",
			v.ToTime(arrow.Nanosecond).Format(time.RFC3339Nano))
	})
}
