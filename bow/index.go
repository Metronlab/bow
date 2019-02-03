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

	dType, err := getTypeFromArrowType(b.Schema().Field(colIndex).Type.Name())
	if err != nil {
		panic(err)
	}

	m := make(map[interface{}][]int)
	for i:=int64(0); i < b.NumRows(); i++ {
		if _, ok := m[b.GetValue(colIndex, int(i))]; !ok {
			m[b.GetValue(colIndex, int(i))] = []int{int(i)}
		} else {
			m[b.GetValue(colIndex, int(i))] = append(m[b.GetValue(colIndex, int(i))], int(i))
		}
	}

	if b.indexes == nil {
		b.indexes = map[string]index{}
	}
	b.indexes[colName] = index{t:dType, m:m}
}

func (b *bow) getIndex(name string, val interface{}) ([]int, bool) {
	res, ok  := b.indexes[name].m[val]
	return res, ok
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
