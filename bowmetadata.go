package bow

import (
	"fmt"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
)

// Metadata is wrapping arrow.Metadata.
type Metadata struct {
	arrow.Metadata
}

// NewMetadata returns a new Metadata.
func NewMetadata(keys, values []string) Metadata {
	return Metadata{arrow.NewMetadata(keys, values)}
}

// NewBowWithMetadata returns a new Bow from Metadata and Series.
func NewBowWithMetadata(metadata Metadata, series ...Series) (Bow, error) {
	rec, err := newRecord(metadata, series...)
	if err != nil {
		return nil, fmt.Errorf("newRecord: %w", err)
	}

	return &bow{Record: rec}, nil
}

// Metadata return a copy of the bow Schema Metadata.
func (b *bow) Metadata() Metadata {
	return NewMetadata(
		b.Schema().Metadata().Keys(),
		b.Schema().Metadata().Values())
}

// SetMetadata sets a value for a given key and return a Bow with freshly created Metadata.
func (b *bow) SetMetadata(key, value string) Bow {
	m := b.Metadata()
	m = m.Set(key, value)
	return &bow{Record: array.NewRecord(
		arrow.NewSchema(b.Schema().Fields(), &m.Metadata),
		b.Columns(),
		b.Record.NumRows())}
}

// WithMetadata replaces the bow original Metadata.
func (b *bow) WithMetadata(metadata Metadata) Bow {
	m := arrow.NewMetadata(metadata.Keys(), metadata.Values())
	return &bow{Record: array.NewRecord(
		arrow.NewSchema(b.Schema().Fields(), &m),
		b.Columns(),
		b.Record.NumRows())}
}

// Set returns a new Metadata with the key/value pair set.
// If the key already exists, it replaces its value.
func (m *Metadata) Set(newKey, newValue string) Metadata {
	keys := m.Keys()
	values := m.Values()
	keyIndex := m.FindKey(newKey)

	if keyIndex == -1 {
		keys = append(keys, newKey)
		values = append(values, newValue)
	} else {
		values[keyIndex] = newValue
	}

	return Metadata{arrow.NewMetadata(keys, values)}
}

// SetMany returns a new Metadata with the key/value pairs set.
// If a key already exists, it replaces its value.
func (m *Metadata) SetMany(newKeys, newValues []string) Metadata {
	if len(newKeys) != len(newValues) {
		panic("metadata len mismatch")
	}
	if len(newKeys) == 0 {
		return *m
	}

	keys := m.Keys()
	values := m.Values()

	for i, newKey := range newKeys {
		newKeyIndex := m.FindKey(newKey)
		if newKeyIndex == -1 {
			keys = append(keys, newKey)
			values = append(values, newValues[i])
		} else {
			values[newKeyIndex] = newValues[i]
		}
	}

	return Metadata{arrow.NewMetadata(keys, values)}
}
