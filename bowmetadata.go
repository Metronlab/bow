package bow

import (
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
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

func (b *bow) Metadata() Metadata {
	return NewMetadata(
		b.Schema().Metadata().Keys(),
		b.Schema().Metadata().Values())
}

func (b *bow) SetMetadata(key, value string) Bow {
	metadata := b.Metadata()
	metadata = metadata.Set(key, value)
	return &bow{Record: array.NewRecord(
		arrow.NewSchema(b.Schema().Fields(), &metadata.Metadata),
		b.Columns(),
		b.Record.NumRows())}
}

func (md *Metadata) Set(key, value string) Metadata {
	srcKeys := md.Keys()
	srcValues := md.Values()
	srcKeyIdx := md.FindKey(key)
	if srcKeyIdx == -1 {
		srcKeys = append(srcKeys, key)
		srcValues = append(srcValues, value)
	} else {
		srcValues[srcKeyIdx] = value
	}
	return Metadata{arrow.NewMetadata(srcKeys, srcValues)}
}

func (md *Metadata) SetMany(keys, values []string) Metadata {
	if len(keys) != len(values) {
		panic("metadata len mismatch")
	}
	if len(keys) == 0 {
		return *md
	}

	srcKeys := md.Keys()
	srcValues := md.Values()
	for i, key := range keys {
		srcKeyIdx := md.FindKey(key)
		if srcKeyIdx == -1 {
			srcKeys = append(srcKeys, key)
			srcValues = append(srcValues, values[i])
		} else {
			srcValues[srcKeyIdx] = values[i]
		}
	}
	return Metadata{arrow.NewMetadata(srcKeys, srcValues)}
}
