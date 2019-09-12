package bow

import (
	"github.com/apache/arrow/go/arrow"
)

type Type int

const (
	//Unknown is placed first to be by default
	// when allocating Type or []Type
	Unknown = Type(iota)

	// Float64 and following types are native arrow type supported by bow
	Float64
	Int64
	Bool
	//string not handled yet

	//InputDependent is used in transformation like aggregation
	// when output type is infer with input type
	InputDependent
	//IteratorDependent is used in transformation like aggregation
	// when output type is infer with iteratorType
	IteratorDependent
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
	}
	if ok {
		return val
	}
	return nil
}

func (t Type) String() string {
	switch t {
	case Unknown:
		return "Unknown"
	case Float64:
		return "Float64"
	case Int64:
		return "Int64"
	case Bool:
		return "Bool"
	default:
		return "Undefined"
	}
}

func getTypeFromArrowType(arrowType string) Type {
	switch arrowType {
	case arrow.PrimitiveTypes.Float64.Name():
		return Float64
	case arrow.PrimitiveTypes.Int64.Name():
		return Int64
	case arrow.FixedWidthTypes.Boolean.Name():
		return Bool
	default:
		return Unknown
	}
}
