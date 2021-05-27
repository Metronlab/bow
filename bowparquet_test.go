package bow

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParquet(t *testing.T) {
	b, err := ParquetFileRead("data.parquet")
	assert.NoError(t, err)
	fmt.Printf("RES\n%+v\n", b)
}
