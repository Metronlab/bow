package bow

import (
	"fmt"

	"github.com/apache/arrow/go/arrow"
)

type Metadata struct {
	arrow.Metadata
}

func NewMetadata(keys, values []string) Metadata {
	return Metadata{arrow.NewMetadata(keys, values)}
}

func NewBowWithMetadata(metadata Metadata, series ...Series) (Bow, error) {
	rec, err := newRecord(metadata, series...)
	if err != nil {
		return nil, fmt.Errorf("bow.NewBowWithMetadata: %w", err)
	}

	return &bow{Record: rec}, nil
}

func (b *bow) GetMetadata() Metadata {
	return NewMetadata(
		b.Schema().Metadata().Keys(),
		b.Schema().Metadata().Values())
}
