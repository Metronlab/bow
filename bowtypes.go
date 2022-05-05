package bow

import (
	"fmt"

	"github.com/apache/arrow/go/v8/arrow"
)

type Type int

// How to add a Type:
// - Seek corresponding arrow.DataType and add it in `mapBowToArrowTypes`
// - add a convert function with desired logic and add case in other conversion func
// - add necessary case in buffer file
// - complete GetValue bow method

const (
	// Unknown is placed first to be the default when allocating Type or []Type.
	Unknown = Type(iota)

	// Float64 and following types are native arrow type supported by bow.
	Float64
	Int64
	Boolean
	String

	// InputDependent is used in aggregations when the output type is dependent on the input type.
	InputDependent

	// IteratorDependent is used in aggregations when the output type is dependent on the iterator type.
	IteratorDependent
)

var (
	mapBowToArrowTypes = map[Type]arrow.DataType{
		Float64: arrow.PrimitiveTypes.Float64,
		Int64:   arrow.PrimitiveTypes.Int64,
		Boolean: arrow.FixedWidthTypes.Boolean,
		String:  arrow.BinaryTypes.String,
	}
	mapArrowNameToBowTypes = func() map[string]Type {
		res := make(map[string]Type)
		for bowType, arrowDataType := range mapBowToArrowTypes {
			res[arrowDataType.Name()] = bowType
		}
		return res
	}()
	allType = func() []Type {
		res := make([]Type, InputDependent-1)
		for typ := Type(1); typ < InputDependent; typ++ {
			res[typ-1] = typ
		}
		return res
	}()
)

func (t Type) Convert(input interface{}) interface{} {
	var output interface{}
	var ok bool
	switch t {
	case Float64:
		output, ok = ToFloat64(input)
	case Int64:
		output, ok = ToInt64(input)
	case Boolean:
		output, ok = ToBoolean(input)
	case String:
		output, ok = ToString(input)
	}
	if ok {
		return output
	}
	return nil
}

// IsSupported ensures that the Type t is currently supported by Bow and matches a convertible concrete type.
func (t Type) IsSupported() bool {
	_, ok := mapBowToArrowTypes[t]
	return ok
}

// String returns the string representation of the Type t.
func (t Type) String() string {
	at, ok := mapBowToArrowTypes[t]
	if !ok {
		return "undefined"
	}
	return fmt.Sprintf("%s", at)
}

func getBowTypeFromArrowFingerprint(fingerprint string) Type {
	for bowType, arrowType := range mapBowToArrowTypes {
		if arrowType.Fingerprint() == fingerprint {
			return bowType
		}
	}
	return Unknown
}

func getBowTypeFromArrowName(name string) Type {
	for bowType, arrowType := range mapBowToArrowTypes {
		if arrowType.Name() == name {
			return bowType
		}
	}
	return Unknown
}

// GetAllTypes returns all Bow types.
func GetAllTypes() []Type {
	res := make([]Type, len(allType))
	copy(res, allType)
	return res
}
