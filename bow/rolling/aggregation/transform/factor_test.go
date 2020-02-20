package transform

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFactor(t *testing.T) {
	transform := Factor(0.1)

	t.Run("invalid input", func(t *testing.T) {
		res, err := transform("11")
		assert.EqualError(t, err, "factor: invalid type string")
		assert.Nil(t, res)
	})

	t.Run("preserve nil", func(t *testing.T) {
		res, err := transform(nil)
		assert.Nil(t, err)
		assert.Nil(t, res)
	})

	t.Run("preserve int64", func(t *testing.T) {
		res, err := transform(int64(11))
		assert.Nil(t, err)
		assert.Equal(t, int64(1), res)
	})

	t.Run("preserve float64", func(t *testing.T) {
		res, err := transform(11.)
		assert.Nil(t, err)
		assert.Equal(t, 1.1, res)
	})
}
