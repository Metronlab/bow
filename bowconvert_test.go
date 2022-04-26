package bow

import (
	"testing"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToBool(t *testing.T) {
	var v bool
	var ok bool

	v, ok = ToBool(true)
	require.True(t, ok)
	assert.Equal(t, true, v)

	v, ok = ToBool(false)
	require.True(t, ok)
	assert.Equal(t, false, v)

	v, ok = ToBool("true")
	require.True(t, ok)
	assert.Equal(t, true, v)

	v, ok = ToBool("True")
	require.True(t, ok)
	assert.Equal(t, true, v)

	v, ok = ToBool("false")
	require.True(t, ok)
	assert.Equal(t, false, v)

	v, ok = ToBool("False")
	require.True(t, ok)
	assert.Equal(t, false, v)

	v, ok = ToBool(1)
	require.True(t, v)
	require.True(t, ok)
	v, ok = ToBool(0)
	require.False(t, v)
	require.True(t, ok)

	v, ok = ToBool(1.)
	require.True(t, v)
	require.True(t, ok)
	v, ok = ToBool(0.)
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
	var v arrow.Timestamp
	var ok bool

	v, ok = ToTimestamp(true)
	require.True(t, ok)
	assert.Equal(t, arrow.Timestamp(1), v)

	v, ok = ToTimestamp(false)
	require.True(t, ok)
	assert.Equal(t, arrow.Timestamp(0), v)

	v, ok = ToTimestamp(0.)
	require.True(t, ok)
	assert.Equal(t, arrow.Timestamp(0), v)

	v, ok = ToTimestamp(0)
	require.True(t, ok)
	assert.Equal(t, arrow.Timestamp(0), v)

	v, ok = ToTimestamp("0")
	require.True(t, ok)
	assert.Equal(t, arrow.Timestamp(0), v)
}
