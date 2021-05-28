package bow

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParquet(t *testing.T) {
	bBefore, err := NewBowFromParquet("data-before.parquet")
	assert.NoError(t, err)
	//fmt.Printf("BOW: %d ROWS\n%+v\n", bBefore.NumRows(), bBefore.Schema().String())

	err = bBefore.WriteParquet("data-after.parquet")
	assert.NoError(t, err)

	bAfter, err := NewBowFromParquet("data-after.parquet")
	assert.NoError(t, err)

	assert.Equal(t, bBefore.String(), bAfter.String())
}
