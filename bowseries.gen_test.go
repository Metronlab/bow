package bow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSeriesFromColBasedInterfaces(t *testing.T) {
	for _, typ := range allType {
		t.Run(typ.String(), func(t *testing.T) {
			testcase := []interface{}{typ.Convert(0), nil}
			res, err := NewBow(NewSeriesFromInterfaces(typ.String(), typ, testcase))
			require.NoError(t, err)
			assert.Equal(t, typ.Convert(0), res.GetValue(0, 0))
			assert.Equal(t, nil, res.GetValue(0, 1))
		})
	}
}
