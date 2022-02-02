package bow

import (
	"fmt"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
)

// Metadata is an arrow metadata wrapping
// often used to reference information about indexes, sort, units...
// Metadata is mutable but copied at creation and at assignation in arrow schema
// Recommended usage is to settle on a key for your business and marshall / unmarshall with json for instance
// to enrich the value always for the same key.
// You can find example of usage by reading parquet or reading arrow file issued by panda in python.
// Consider "bow" key as reserved for the future.
type Metadata struct {
	arrow.Metadata
}

func NewMetadata(keys, values []string) Metadata {
	return Metadata{arrow.NewMetadata(keys, values)}
}

func NewMetadataFromMap(m map[string]string) Metadata {
	return Metadata{arrow.MetadataFrom(m)}
}

func NewBowWithMetadata(metadata Metadata, series ...Series) (Bow, error) {
	rec, err := newRecord(metadata, series...)
	if err != nil {
		return nil, fmt.Errorf("bow.NewBowWithMetadata: %w", err)
	}

	return &bow{Record: rec}, nil
}

// Metadata return a copy of schema metadata.
func (b *bow) Metadata() Metadata {
	return NewMetadata(
		b.Schema().Metadata().Keys(),
		b.Schema().Metadata().Values())
}

// SetMetadata Set a value for a given key and return a Bow with freshly created metadata
func (b *bow) SetMetadata(key, value string) Bow {
	metadata := b.Metadata()
	metadata = metadata.Set(key, value)
	return &bow{Record: array.NewRecord(
		arrow.NewSchema(b.Schema().Fields(), &metadata.Metadata),
		b.Columns(),
		b.Record.NumRows())}
}

// WithMetadata completely replace original Metadata
// Use with caution to avoid information loss for metadata issued by other sources
// A copy is assigned, so you can still mutate metadata given as parameter
func (b *bow) WithMetadata(metadata Metadata) Bow {
	m := arrow.NewMetadata(metadata.Keys(), metadata.Values())
	return &bow{Record: array.NewRecord(
		arrow.NewSchema(b.Schema().Fields(), &m),
		b.Columns(),
		b.Record.NumRows())}
}

// Set mutate the Metadata in case key already exists and return a fresh copy
// with given key and value assigned
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

// SetMany mutate the Metadata in case key already exists and return a fresh copy
// with given keys and values assigned
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
