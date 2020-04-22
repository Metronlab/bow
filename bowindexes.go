package bow

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

func (b *bow) getIndex(name string, val interface{}) ([]int, bool) {
	index, ok := b.indexes[name]
	if !ok {
		return []int{}, false
	}
	res, ok := index.m[val]
	return res, ok
}

func (b *bow) getRightBowIndexesAtRow(b2 *bow, commonColumns map[string]struct{}, rowIndex int) []int {
	var possibleIndexes [][]int
	for name := range commonColumns {
		val := b.GetValue(b.Schema().FieldIndex(name), rowIndex)
		if val == nil {
			return []int{}
		}

		indexes, ok := b2.getIndex(name, val)
		if !ok {
			return []int{}
		}

		possibleIndexes = append(possibleIndexes, indexes)
	}
	return commonInt(possibleIndexes...)
}

func commonInt(ints ...[]int) []int {
	if len(ints) == 0 {
		return []int{}
	}

	res := ints[0]
	if len(ints) == 1 {
		return res
	}

	for _, intss := range ints[1:] {
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
