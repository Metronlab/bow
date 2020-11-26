package bow

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
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

	t.Run("EncodeBowToJSONBody", func(t *testing.T) {
		t.Run("empty", func(t *testing.T) {
			jsonB, err := EncodeBowToJSONBody(emptyBow)
			assert.NoError(t, err)

			respBody, err := ioutil.ReadAll(jsonB)
			require.NoError(t, err)

			var jsonOutputB jsonRecord
			err = json.Unmarshal(respBody, &jsonOutputB)
			require.NoError(t, err)

			expected := jsonRecord{}
			assert.Equal(t, expected, jsonOutputB)
		})

		t.Run("simple", func(t *testing.T) {
			jsonB, err := EncodeBowToJSONBody(simpleBow)
			assert.NoError(t, err)

			respBody, err := ioutil.ReadAll(jsonB)
			require.NoError(t, err)

			var jsonOutputB jsonRecord
			err = json.Unmarshal(respBody, &jsonOutputB)
			require.NoError(t, err)

			expected := jsonRecord{
				Schema: jsonSchema{
					[]jsonField{
						{Name: "a", Type: "int64"},
						{Name: "b", Type: "float64"},
						{Name: "c", Type: "bool"},
					},
				},
				Data: []map[string]interface{}{
					{"a": float64(100), "b": float64(200), "c": false},
					{"a": float64(110), "b": float64(220), "c": true},
					{"a": float64(111), "b": float64(222), "c": false},
				},
			}
			assert.Equal(t, expected, jsonOutputB)
		})
	})

	t.Run("UnmarshalJSON", func(t *testing.T) {
		t.Run("empty", func(t *testing.T) {
			jsonB, err := json.Marshal(emptyBow)
			require.NoError(t, err)

			decodedBow, err := UnmarshalJSON(jsonB)
			require.NoError(t, err)

			assert.True(t, emptyBow.Equal(decodedBow),
				fmt.Sprintf("have:\n%vexpect:\n%v", decodedBow, emptyBow))
		})

		t.Run("simple", func(t *testing.T) {
			jsonB, err := json.Marshal(simpleBow)
			require.NoError(t, err)

			decodedBow, err := UnmarshalJSON(jsonB)
			require.NoError(t, err)

			assert.True(t, simpleBow.Equal(decodedBow),
				fmt.Sprintf("have:\n%vexpect:\n%v", decodedBow, simpleBow))
		})
	})

	t.Run("DecodeJSONRespToBow", func(t *testing.T) {
		t.Run("empty", func(t *testing.T) {
			jsonB, err := EncodeBowToJSONBody(emptyBow)
			require.NoError(t, err)

			decodedBow, err := DecodeJSONRespToBow(jsonB)
			assert.NoError(t, err)

			assert.True(t, emptyBow.Equal(decodedBow),
				fmt.Sprintf("have:\n%vexpect:\n%v", decodedBow, emptyBow))
		})

		t.Run("simple", func(t *testing.T) {
			jsonB, err := EncodeBowToJSONBody(simpleBow)
			require.NoError(t, err)

			decodedBow, err := DecodeJSONRespToBow(jsonB)
			assert.NoError(t, err)

			assert.True(t, simpleBow.Equal(decodedBow),
				fmt.Sprintf("have:\n%vexpect:\n%v", decodedBow, simpleBow))
		})
	})
}
