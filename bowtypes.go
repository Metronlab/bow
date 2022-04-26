package bow

import (
	"fmt"

	"github.com/apache/arrow/go/v8/arrow"
)

type Type int

// How to add a Type:
// - Seek corresponding arrow.Type and add it in `mapArrowFingerprintToBowTypes`
// - add a convert function with desired logic and add case in other conversion func
// - add necessary case in buffer file
// - complete GetValue bow method

const (
	// Unknown is placed first to be by default
	// when allocating Type or []Type
	Unknown = Type(iota)

	// Float64 and following types are native arrow type supported by bow
	Float64
	Int64
	Bool
	String
	TimestampSec
	TimestampMilli
	TimestampMicro
	TimestampNano

	// InputDependent is used in transformation like aggregation
	// when output type is infer with input type
	InputDependent

	// IteratorDependent is used in transformation like aggregation
	// when output type is infer with iteratorType
	IteratorDependent
)

var (
	mapBowToArrowDataTypes = map[Type]arrow.DataType{
		Float64:        arrow.PrimitiveTypes.Float64,
		Int64:          arrow.PrimitiveTypes.Int64,
		Bool:           arrow.FixedWidthTypes.Boolean,
		String:         arrow.BinaryTypes.String,
		TimestampSec:   arrow.FixedWidthTypes.Timestamp_s,
		TimestampMilli: arrow.FixedWidthTypes.Timestamp_ms,
		TimestampMicro: arrow.FixedWidthTypes.Timestamp_us,
		TimestampNano:  arrow.FixedWidthTypes.Timestamp_ns,
	}
	mapArrowFingerprintToBowTypes = map[string]Type{
		arrow.PrimitiveTypes.Float64.Fingerprint():       Float64,
		arrow.PrimitiveTypes.Int64.Fingerprint():         Int64,
		arrow.FixedWidthTypes.Boolean.Fingerprint():      Bool,
		arrow.BinaryTypes.String.Fingerprint():           String,
		arrow.FixedWidthTypes.Timestamp_s.Fingerprint():  TimestampSec,
		arrow.FixedWidthTypes.Timestamp_ms.Fingerprint(): TimestampMilli,
		arrow.FixedWidthTypes.Timestamp_us.Fingerprint(): TimestampMicro,
		arrow.FixedWidthTypes.Timestamp_ns.Fingerprint(): TimestampNano,
	}
	mapArrowNameToBowTypes = map[string]Type{
		arrow.PrimitiveTypes.Float64.Name():       Float64,
		arrow.PrimitiveTypes.Int64.Name():         Int64,
		arrow.FixedWidthTypes.Boolean.Name():      Bool,
		arrow.BinaryTypes.String.Name():           String,
		arrow.FixedWidthTypes.Timestamp_s.Name():  TimestampSec,
		arrow.FixedWidthTypes.Timestamp_ms.Name(): TimestampMilli,
		arrow.FixedWidthTypes.Timestamp_us.Name(): TimestampMicro,
		arrow.FixedWidthTypes.Timestamp_ns.Name(): TimestampNano,
	}
	allType = func() []Type {
		res := make([]Type, InputDependent-1)
		for typ := Type(1); typ < InputDependent; typ++ {
			res[typ-1] = typ
		}
		return res
	}()
)

func (t Type) Convert(i interface{}) interface{} {
	var val interface{}
	var ok bool
	switch t {
	case Float64:
		val, ok = ToFloat64(i)
	case Int64:
		val, ok = ToInt64(i)
	case Bool:
		val, ok = ToBool(i)
	case String:
		val, ok = ToString(i)
	case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
		val, ok = ToTimestamp(i)
	}
	if ok {
		return val
	}
	return nil
}

// IsSupported ensures that the type is currently supported by Bow
// and match a convertible concrete type.
func (t Type) IsSupported() bool {
	_, ok := mapBowToArrowDataTypes[t]
	return ok
}

func (t Type) String() string {
	at, ok := mapBowToArrowDataTypes[t]
	if !ok {
		return "undefined"
	}
	return fmt.Sprintf("%s", at)
}

func getBowTypeFromArrowFingerprint(fingerprint string) Type {
	typ, ok := mapArrowFingerprintToBowTypes[fingerprint]
	if !ok {
		return Unknown
	}
	return typ
}

func getBowTypeFromArrowName(name string) Type {
	typ, ok := mapArrowNameToBowTypes[name]
	if !ok {
		return Unknown
	}
	return typ
}

func GetAllTypes() []Type {
	res := make([]Type, len(allType))
	copy(res, allType)
	return res
}
