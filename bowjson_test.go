package bow

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBow_UnmarshalJSON(t *testing.T) {
	columns := []string{"time", "value", "valueFromJson", "string"}
	ts := make([]Type, len(columns))
	ts[0] = Int64
	rows := [][]interface{}{
		{1, 1.2, json.Number("3"), nil},
		{nil, 1, json.Number("1.2"), 3},
		{json.Number("1.1"), nil, 2, 1.3},
		{nil, "", "test", "string"},
	}

	b, err := NewBowFromColumnBasedInterfaces(columns, ts, rows)
	require.NoError(t, err)

	b.SetMarshalJSONRowBased(true)
	js, err := json.Marshal(b)
	require.NoError(t, err)

	b2test, err := NewBow()
	require.NoError(t, err)

	err = json.Unmarshal(js, &b2test)
	require.NoError(t, err)

	assert.True(t, b.Equal(b2test), fmt.Sprintf("have:\n%vexpect:\n%v", b2test, b))
}
