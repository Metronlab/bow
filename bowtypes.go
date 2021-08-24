package bow

import (
	"github.com/apache/arrow/go/arrow"
)

type Type int

// How to add a Type:
// - Seek corresponding arrow.DataType and add it in `mapArrowToBowTypes`
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
	Boolean
	String

	// InputDependent is used in transformation like aggregation
	// when output type is infer with input type
	InputDependent

	// IteratorDependent is used in transformation like aggregation
	// when output type is infer with iteratorType
	IteratorDependent
)

var (
	mapArrowToBowTypes = map[arrow.DataType]Type{
		arrow.PrimitiveTypes.Float64:  Float64,
		arrow.PrimitiveTypes.Int64:    Int64,
		arrow.FixedWidthTypes.Boolean: Boolean,
		arrow.BinaryTypes.String:      String,
	}
	mapBowToArrowTypes = func() map[Type]arrow.DataType {
		res := make(map[Type]arrow.DataType)
		for arrowDataType, bowType := range mapArrowToBowTypes {
			res[bowType] = arrowDataType
		}
		return res
	}()
	mapArrowNameToBowTypes = func() map[string]Type {
		res := make(map[string]Type)
		for arrowDataType, bowType := range mapArrowToBowTypes {
			res[arrowDataType.Name()] = bowType
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
	case Boolean:
		val, ok = ToBoolean(i)
	case String:
		val, ok = ToString(i)
	}
	if ok {
		return val
	}
	return nil
}

// IsSupported ensures that the type is currently supported by Bow
// and match a convertible concrete type.
func (t Type) IsSupported() bool {
	_, ok := mapBowToArrowTypes[t]
	return ok
}

func (t Type) String() string {
	at, ok := mapBowToArrowTypes[t]
	if !ok {
		return "undefined"
	}
	return at.Name()
}

func getBowTypeFromArrowName(arrowName string) Type {
	typ, ok := mapArrowNameToBowTypes[arrowName]
	if !ok {
		return Unknown
	}
	return typ
}

func getBowTypeFromArrowType(arrowType arrow.DataType) Type {
	typ, ok := mapArrowToBowTypes[arrowType]
	if !ok {
		return Unknown
	}
	return typ
}
