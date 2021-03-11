package bow

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSON(t *testing.T) {
	t.Run("MarshalJSON", func(t *testing.T) {
		t.Run("empty", func(t *testing.T) {
			b := NewBowEmpty()

			byteB, err := b.MarshalJSON()
			require.NoError(t, err)

			jsonB := JSONBow{}
			err = json.Unmarshal(byteB, &jsonB)
			require.NoError(t, err)

			expected := JSONBow{
				Schema: jsonSchema{},
				Data:   []map[string]interface{}{},
			}
			assert.Equal(t, expected, jsonB)
		})

		t.Run("simple", func(t *testing.T) {
			b, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c"},
				[]Type{Int64, Float64, Bool},
				[][]interface{}{
					{100, 200., false},
					{110, 220., true},
					{111, 222., false},
				})
			require.NoError(t, err)

			byteB, err := b.MarshalJSON()
			require.NoError(t, err)

			jsonB := JSONBow{}
			err = json.Unmarshal(byteB, &jsonB)
			require.NoError(t, err)

			expected := JSONBow{
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
			assert.Equal(t, expected, jsonB)
		})
	})

	t.Run("UnmarshalJSON", func(t *testing.T) {
		t.Run("empty", func(t *testing.T) {
			b := NewBowEmpty()

			byteB, err := json.Marshal(b)
			require.NoError(t, err)

			bCopy := b
			err = bCopy.UnmarshalJSON(byteB)
			require.NoError(t, err)

			assert.True(t, b.Equal(bCopy),
				fmt.Sprintf("have:\n%vexpect:\n%v", bCopy, b))
		})

		t.Run("simple", func(t *testing.T) {
			b, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c"},
				[]Type{Int64, Float64, Bool},
				[][]interface{}{
					{100, 200., false},
					{110, 220., true},
					{111, 222., false},
				})
			require.NoError(t, err)

			byteB, err := json.Marshal(b)
			require.NoError(t, err)

			bCopy := b
			err = b.UnmarshalJSON(byteB)
			require.NoError(t, err)

			assert.True(t, b.Equal(bCopy),
				fmt.Sprintf("have:\n%vexpect:\n%v", bCopy, b))
		})

		t.Run("simple no data", func(t *testing.T) {
			b, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c"},
				[]Type{Int64, Float64, Bool},
				[][]interface{}{})
			require.NoError(t, err)

			byteB, err := json.Marshal(b)
			require.NoError(t, err)

			bCopy := b
			err = b.UnmarshalJSON(byteB)
			require.NoError(t, err)

			assert.True(t, b.Equal(bCopy),
				fmt.Sprintf("have:\n%vexpect:\n%v", bCopy, b))
		})
	})
}
