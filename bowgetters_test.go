package bow

import (
	"testing"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBow_GetValue(t *testing.T) {
	colNames := []string{"time", "value", "meta"}
	colTypes := []Type{Int64, Float64, String}
	colData := [][]interface{}{
		{1, 2, 3},
		{1.1, 2.2, 3.3},
		{"", "test", "3.3"},
	}

	b, err := NewBowFromColBasedInterfaces(colNames, colTypes, colData)
	require.NoError(t, err)

	assert.Equal(t, 3.3, b.GetValue(1, 2))
	assert.Equal(t, map[string]interface{}{
		"time":  int64(2),
		"value": 2.2,
		"meta":  "test",
	}, b.GetRow(1))

	res, ok := b.GetFloat64(2, 2)
	assert.True(t, ok)
	assert.Equal(t, 3.3, res)
}

func TestBow_Distinct(t *testing.T) {
	colNames := []string{"time", "value", "meta", "timestamp"}
	colTypes := []Type{Int64, Float64, String, TimestampMilli}
	colData := [][]interface{}{
		{1, 1, 2, nil, 3},
		{1.1, 1.1, 2.2, nil, 3.3},
		{"", "test", "test", nil, "3.3"},
		{1, 1, 2, nil, 3},
	}

	b, err := NewBowFromColBasedInterfaces(colNames, colTypes, colData)
	require.NoError(t, err)

	t.Run(Int64.String(), func(t *testing.T) {
		res := b.Distinct(0)
		expect, err := NewBow(NewSeries("time", Int64, []int64{1, 2, 3}, nil))
		require.NoError(t, err)

		ExpectEqual(t, expect, res)
	})

	t.Run(Float64.String(), func(t *testing.T) {
		res := b.Distinct(1)
		expect, err := NewBow(NewSeries("value", Float64, []float64{1.1, 2.2, 3.3}, nil))
		require.NoError(t, err)

		ExpectEqual(t, expect, res)
	})

	t.Run(String.String(), func(t *testing.T) {
		res := b.Distinct(2)
		expect, err := NewBow(NewSeries("meta", String, []string{"", "3.3", "test"}, nil))
		require.NoError(t, err)

		ExpectEqual(t, expect, res)
	})

	t.Run(TimestampMilli.String(), func(t *testing.T) {
		res := b.Distinct(3)
		expect, err := NewBow(NewSeries("timestamp", TimestampMilli, []arrow.Timestamp{1, 2, 3}, nil))
		require.NoError(t, err)

		ExpectEqual(t, expect, res)
	})
}
