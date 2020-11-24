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

func TestBow_EncodeBowToJSON(t *testing.T) {
	inputBow, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c"}, []Type{Int64, Int64, Int64}, [][]interface{}{
		{100, 200, 300},
		{110, 220, 330},
		{111, 222, 333},
	})
	require.NoError(t, err)

	jsonInputBow, err := EncodeBowToJSON(inputBow)
	assert.NoError(t, err)
	var p []byte
	n, err := jsonInputBow.Read(p)
	assert.NoError(t, err)
	fmt.Printf("p:%+v\nn:%+v\n", p, n)
}
