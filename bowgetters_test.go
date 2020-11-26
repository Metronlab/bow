package bow

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBow_GetValue(t *testing.T) {
	colNames := []string{"time", "value", "meta"}
	colTypes := []Type{Int64, Float64, String}
	colData := [][]interface{}{
		{1, 2, 3},
		{1.1, 2.2, 3.3},
		{"", "test", "3.3"},
	}

	b, err := NewBowFromColumnBasedInterfaces(colNames, colTypes, colData)
	require.NoError(t, err)

	assert.Equal(t, 3.3, b.GetValue(1, 2))
	assert.Equal(t, 3.3, b.GetValueByName("value", 2))
	assert.Equal(t, map[string]interface{}{
		"time":  int64(2),
		"value": 2.2,
		"meta":  "test",
	}, b.GetRow(1))

	res, ok := b.GetFloat64(2, 2)
	assert.True(t, ok)
	assert.Equal(t, 3.3, res)
}
