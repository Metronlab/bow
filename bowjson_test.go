package bow

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestJSON(t *testing.T) {
	emptyBow := NewBowEmpty()
	emptyBow.SetMarshalJSONRowBased(true)

	simpleBow, err := NewBowFromRowBasedInterfaces(
		[]string{"a", "b", "c"},
		[]Type{Int64, Float64, Bool},
		[][]interface{}{
			{100, 200., false},
			{110, 220., true},
			{111, 222., false},
		})
	require.NoError(t, err)
	simpleBow.SetMarshalJSONRowBased(true)

	t.Run("MarshalJSON", func(t *testing.T) {
		t.Run("empty", func(t *testing.T) {
			jsonB, err := emptyBow.MarshalJSON()
			require.NoError(t, err)

			rec := jsonRecord{}
			err = json.Unmarshal(jsonB, &rec)
			require.NoError(t, err)

			expected := jsonRecord{}
			assert.Equal(t, expected, rec)
		})

		t.Run("simple", func(t *testing.T) {
			jsonB, err := simpleBow.MarshalJSON()
			require.NoError(t, err)

			rec := jsonRecord{}
			err = json.Unmarshal(jsonB, &rec)
			require.NoError(t, err)

			expected := jsonRecord{
				Schema: jsonSchema{
					Fields: []jsonField{
						{Name: "a", Type: "int64"},
						{Name: "b", Type: "float64"},
						{Name: "c", Type: "bool"},
					},
				},
				Data: []map[string]interface{}{
					{"a": 100., "b": 200., "c": false},
					{"a": 110., "b": 220., "c": true},
					{"a": 111., "b": 222., "c": false},
				},
			}

			assert.Equal(t, expected, rec)

		})
	})

	t.Run("UnmarshalJSON", func(t *testing.T) {
		t.Run("empty", func(t *testing.T) {
			jsonB, err := json.Marshal(emptyBow)
			require.NoError(t, err)

			decodedBow := emptyBow
			err = decodedBow.UnmarshalJSON(jsonB)
			require.Error(t, err)
		})

		t.Run("simple", func(t *testing.T) {
			jsonB, err := json.Marshal(simpleBow)
			require.NoError(t, err)

			decodedBow := simpleBow
			err = simpleBow.UnmarshalJSON(jsonB)
			require.NoError(t, err)

			assert.True(t, simpleBow.Equal(decodedBow),
				fmt.Sprintf("have:\n%vexpect:\n%v", decodedBow, simpleBow))
		})
	})
}
