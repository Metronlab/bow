package bow

import (
	"fmt"

	"github.com/apache/arrow/go/arrow"
)

type Type int

const (
	//Unknown is placed first to be by default
	// when allocating Type or []Type
	Unknown = iota
	Float64
	Int64
	Bool
	//string not handled yet
)

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
		return ""
	}
}

func getTypeFromArrowType(arrowType string) (Type, error) {
	switch arrowType {
	case arrow.PrimitiveTypes.Float64.Name():
		return Float64, nil
	case arrow.PrimitiveTypes.Int64.Name():
		return Int64, nil
	case arrow.FixedWidthTypes.Boolean.Name():
		return Bool, nil
	default:
		return Unknown, fmt.Errorf("bow: unknown type %s", arrowType)
	}
}
