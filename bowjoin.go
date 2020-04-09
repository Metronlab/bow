package bow

import (
	"errors"
	"fmt"
	"github.com/apache/arrow/go/arrow/array"
)

func (b *bow) OuterJoin(other Bow, onCol string) Bow {
	left := b
	right, ok := other.(*bow)
	if !ok {
		panic("bow: non bow object passed as argument")
	}
	refColLeft, errLeft := left.GetIndex(onCol)
	if errLeft != nil {
		panic(fmt.Errorf("bow OuterJoin: unknown column name '%s' in left bow", onCol))
	}
	refColRight, errRight := right.GetIndex(onCol)
	if errRight != nil {
		panic(fmt.Errorf("bow OuterJoin: unknown column name '%s' in right bow", onCol))
	}
	if left.GetType(refColLeft) != right.GetType(refColRight) {
		panic(fmt.Errorf("bow OuterJoin: columns '%s' have incompatible types in left and right bows", onCol))
	}
	refDataLeft := left.Record.Column(refColLeft).Data()
	refDataRight := right.Record.Column(refColRight).Data()
	switch left.GetType(refColLeft) {
	case Int64:
		refArr := array.NewInt64Data(refDataLeft)
		for i := 0; i < refArr.Len(); i++ {
			if refArr.IsNull(i) {
				panic(fmt.Errorf("bow OuterJoin: column '%s' has null values in left bow", onCol))
			}
		}
		refArr2 := array.NewInt64Data(refDataRight)
		for i := 0; i < refArr2.Len(); i++ {
			if refArr2.IsNull(i) {
				panic(fmt.Errorf("bow OuterJoin: column '%s' has null values in right bow", onCol))
			}
		}
	case Float64:
		refArr := array.NewFloat64Data(refDataLeft)
		for i := 0; i < refArr.Len(); i++ {
			if refArr.IsNull(i) {
				panic(fmt.Errorf("bow OuterJoin: column '%s' has null values in left bow", onCol))
			}
		}
		refArr2 := array.NewFloat64Data(refDataRight)
		for i := 0; i < refArr2.Len(); i++ {
			if refArr2.IsNull(i) {
				panic(fmt.Errorf("bow OuterJoin: column '%s' has null values in right bow", onCol))
			}
		}
	}
	sorted, _ := left.IsColSorted(refColLeft)
	if !sorted {
		panic(fmt.Errorf("bow OuterJoin: column '%s' in left bow is not sorted", onCol))
	}
	commonCols := make(map[string][]int)
	for _, lField := range left.Schema().Fields() {
		rField, ok := right.Schema().FieldByName(lField.Name)
		if !ok {
			continue
		}
		if rField.Type.ID() != lField.Type.ID() {
			panic(errors.New("bow OuterJoin: left and right bow on join columns are of incompatible types: " + lField.Name))
		}
		commonCols[lField.Name] = append(commonCols[lField.Name], left.Schema().FieldIndex(lField.Name))
		commonCols[lField.Name] = append(commonCols[lField.Name], right.Schema().FieldIndex(rField.Name))
	}
	return left
}

// TODO: used series directly
// For each resulting row, every values is filled first with all left bow columns then right uncommon columns
// If several values are present on right on same indexes, the left indexes/values will be duplicated
// left bow:         right bow:
// index col         index col2
// 1     1           1     1
//                   1     2
// result:
// index col col2
// 1     1   1
// 1     1   2
func (b *bow) InnerJoin(other Bow) Bow {
	b2, ok := other.(*bow)
	if !ok {
		panic("bow: non bow object passed as argument")
	}
	commonColumns := map[string]struct{}{}
	for _, lField := range b.Schema().Fields() {
		rField, ok := b2.Schema().FieldByName(lField.Name)
		if !ok {
			continue
		}
		if rField.Type.ID() != lField.Type.ID() {
			panic(errors.New("bow: left and right bow on join columns are of incompatible types: " + lField.Name))
		}
		commonColumns[lField.Name] = struct{}{}
	}
	var rColIndexes []int
	for i, rField := range b2.Schema().Fields() {
		if _, ok := commonColumns[rField.Name]; !ok {
			rColIndexes = append(rColIndexes, i)
		}
	}
	for name := range commonColumns {
		b2.newIndex(name)
	}
	resultInterfaces := make([][]interface{}, len(b.Schema().Fields())+len(rColIndexes))
	for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
		for _, rValIndex := range b.getRightBowIndexesAtRow(b2, commonColumns, rowIndex) {
			for colIndex := range b.Schema().Fields() {
				resultInterfaces[colIndex] = append(resultInterfaces[colIndex], b.GetValue(colIndex, rowIndex))
			}
			for i, rColIndex := range rColIndexes {
				resultInterfaces[len(b.Schema().Fields())+i] =
					append(resultInterfaces[len(b.Schema().Fields())+i], b2.GetValue(rColIndex, rValIndex))
			}
		}
	}
	colNames := make([]string, len(b.Schema().Fields())+len(rColIndexes))
	colTypes := make([]Type, len(b.Schema().Fields())+len(rColIndexes))
	for i, f := range b.Schema().Fields() {
		colNames[i] = f.Name
		colTypes[i] = b.GetType(i)
	}
	for i, index := range rColIndexes {
		colNames[len(b.Schema().Fields())+i] = b2.Schema().Field(index).Name
		colTypes[len(b.Schema().Fields())+i] = b2.GetType(index)
	}
	res, err := NewBowFromColumnBasedInterfaces(colNames, colTypes, resultInterfaces)
	if err != nil {
		panic(err)
	}
	return res
}

func (b *bow) getRightBowIndexesAtRow(b2 *bow, commonColumns map[string]struct{}, rowIndex int) []int {
	var possibleIndexes [][]int
	for name := range commonColumns {
		val := b.GetValue(b.Schema().FieldIndex(name), rowIndex)
		if val == nil {
			return []int{}
		}
		index, ok := b2.indexes[name]
		if !ok {
			return []int{}
		}
		indexes, ok := index.m[val]
		if !ok {
			return []int{}
		}
		possibleIndexes = append(possibleIndexes, indexes)
	}
	if len(possibleIndexes) == 0 {
		return []int{}
	}
	res := possibleIndexes[0]
	if len(possibleIndexes) == 1 {
		return res
	}
	for _, intss := range possibleIndexes[1:] {
		start := res
		res = []int{}
		for _, i := range intss {
			for _, j := range start {
				if i == j {
					res = append(res, i)
				}
			}
		}
	}
	return res
}

type index struct {
	t Type
	m map[interface{}][]int
}

func (b *bow) newIndex(colName string) {
	if _, ok := b.Schema().FieldByName(colName); !ok {
		panic("bow: try to build index on non existing columns")
	}
	// return if index already exists
	if _, ok := b.indexes[colName]; ok {
		return
	}
	colIndex := b.Schema().FieldIndex(colName)
	dType := b.GetType(colIndex)
	m := make(map[interface{}][]int)
	for i := 0; i < b.NumRows(); i++ {
		val := b.GetValue(colIndex, i)
		if val == nil {
			continue
		}
		if _, ok := m[val]; !ok {
			m[val] = []int{i}
		} else {
			m[val] = append(m[val], i)
		}
	}
	if b.indexes == nil {
		b.indexes = map[string]index{}
	}
	b.indexes[colName] = index{t: dType, m: m}
}
