package bow

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetAllTypes(t *testing.T) {
	cp := GetAllTypes()
	cp[0] = 10
	assert.NotEqual(t, allType, cp)
}
