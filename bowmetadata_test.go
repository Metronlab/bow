package bow

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBow_WithMetadata(t *testing.T) {
	t.Run("adding meta should not modify bow, but correctly change schema", func(t *testing.T) {
		metadata := NewMetadata([]string{"testKey"}, []string{"testValue"})
		b, _ := NewBow(NewSeries("test", Int64, []int64{1, 2}, nil))

		res := b.WithMetadata(metadata)
		assert.True(t, res.Metadata().Equal(metadata.Metadata),
			"expected %q have %q", metadata.String(), b.Metadata().String())
		assert.Equal(t, 0, b.Metadata().Len())
		assert.Equal(t, 1, res.Metadata().Len())
	})
}

func TestMetadataSetMany(t *testing.T) {
	t.Run("single set on existing key", func(t *testing.T) {
		metadata := NewMetadata([]string{"testKey"}, []string{"testValue"})
		expected := NewMetadata([]string{"testKey"}, []string{"updatedValue"})

		res := metadata.SetMany([]string{"testKey"}, []string{"updatedValue"})
		assert.Equal(t, expected, res, "expected %q have %q", expected.String(), res.String())
	})

	t.Run("single set on new key", func(t *testing.T) {
		metadata := NewMetadata([]string{"testKey1"}, []string{"testValue1"})
		expected := NewMetadata([]string{"testKey1", "testKey2"}, []string{"testValue1", "testValue2"})

		res := metadata.SetMany([]string{"testKey2"}, []string{"testValue2"})
		assert.Equal(t, expected, res, "expected %q have %q", expected.String(), res.String())
	})

	t.Run("set many", func(t *testing.T) {
		metadata := NewMetadata(
			[]string{"testKey1", "testKey2", "testKey3"},
			[]string{"testValue1", "testValue2", "testValue3"})

		expectedKeys := []string{"testKey1", "testKey2", "testKey3", "testKey4", "testKey5", "testKey6"}
		expectedValues := []string{"testValue1", "updatedValue2", "testValue3", "testValue4", "testValue5", "testValue6"}
		expected := NewMetadata(expectedKeys, expectedValues)

		res := metadata.SetMany(
			[]string{"testKey2", "testKey4", "testKey5", "testKey6"},
			[]string{"updatedValue2", "testValue4", "testValue5", "testValue6"})
		assert.Equal(t, expected, res, "expected %q have %q", expected.String(), res.String())
	})
}

func TestMetadataSet(t *testing.T) {
	t.Run("single set on existing key", func(t *testing.T) {
		metadata := NewMetadata([]string{"testKey"}, []string{"testValue"})
		expected := NewMetadata([]string{"testKey"}, []string{"updatedValue"})

		res := metadata.Set("testKey", "updatedValue")
		assert.Equal(t, expected, res, "expected %q have %q", expected.String(), res.String())
	})

	t.Run("single set on new key", func(t *testing.T) {
		metadata := NewMetadata([]string{"testKey1"}, []string{"testValue1"})
		expected := NewMetadata([]string{"testKey1", "testKey2"}, []string{"testValue1", "testValue2"})

		res := metadata.Set("testKey2", "testValue2")
		assert.Equal(t, expected, res, "expected %q have %q", expected.String(), res.String())
	})
}
