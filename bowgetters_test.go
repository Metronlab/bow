package bow

import (
	"testing"

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

func TestBow_FindFirst(t *testing.T) {
	const (
		time  = "time"
		value = "value"
		meta  = "meta"
		valid = "valid"
	)
	colNames := []string{time, value, meta, valid}
	colTypes := []Type{Int64, Float64, String, Bool}
	colData := [][]interface{}{
		{1, 2, 3, 4, 5},
		{1.1, nil, 3.3, 4.4, 3.3},
		{"one", "two", nil, "four", "four"},
		{false, true, false, nil, true},
	}

	b, err := NewBowFromColBasedInterfaces(colNames, colTypes, colData)
	require.NoError(t, err)

	type testCase struct {
		name          string
		searchedCol   string
		searchedValue interface{}
		expectedIndex int
	}
	tests := []testCase{
		{
			name:          "valid int",
			searchedCol:   time,
			searchedValue: 2,
			expectedIndex: 1,
		},
		{
			name:          "valid float",
			searchedCol:   value,
			searchedValue: 3.3,
			expectedIndex: 2,
		},
		{
			name:          "valid string",
			searchedCol:   meta,
			searchedValue: "four",
			expectedIndex: 3,
		},
		{
			name:          "valid bool",
			searchedCol:   valid,
			searchedValue: true,
			expectedIndex: 1,
		},
		{
			name:          "not found float",
			searchedCol:   value,
			searchedValue: 3.314159265359,
			expectedIndex: -1,
		},
		{
			name:          "unsupported type complex64",
			searchedCol:   value,
			searchedValue: complex(3.14, 19.92),
			expectedIndex: -1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			colIndices := b.GetColIndices(test.searchedCol)
			require.Equal(t, 1, len(colIndices))

			res := b.FindFirst(colIndices[0], test.searchedValue)
			assert.Equal(t, res, test.expectedIndex, "expected: %q have: %q", test.expectedIndex, res)
		})
	}

}
