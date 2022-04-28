package bow

import (
	"github.com/apache/arrow/go/v8/arrow"
)

type Type int

// How to add a Type:
// - Seek corresponding arrow.DataType and add it in `mapArrowToBowTypes`
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
	allType = func() []Type {
		res := make([]Type, InputDependent-1)
		for typ := Type(1); typ < InputDependent; typ++ {
			res[typ-1] = typ
		}
		return res
	}()
)

// ArrowType returns the arrow.DataType from the Bow Type.
func (t Type) ArrowType() arrow.DataType {
	return mapBowToArrowTypes[t]
}

// Convert attempts to convert the `input` value to the Type t.
// Returns nil if it fails.
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

// GetAllTypes returns all Bow types.
func GetAllTypes() []Type {
	res := make([]Type, len(allType))
	copy(res, allType)
	return res
}
