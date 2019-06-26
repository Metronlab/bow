package aggregation

// TODO: to be discussed
//
//func Difference(col string) rolling.ColumnAggregation {
//	first := First(col).Func()
//	last := Last(col).Func()
//
//	// TODO : difference with next windows
//	return rolling.NewColumnAggregation(col, bow.Float64,
//		func(col int, w bow.Window) (interface{}, error) {
//			if w.Bow.NumRows() == 0 {
//				return nil, nil
//			}
//
//			// TODO: use a getNextVal
//			value, row := getNextFloat64(w.Bow, col, 0)
//			if row >= 0 {
//				return value, nil
//			}
//			return nil, nil
//		})
//}
