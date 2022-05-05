package transformation

import "fmt"

type Func func(interface{}) (interface{}, error)

func Factor(n float64) Func {
	return func(x interface{}) (interface{}, error) {
		switch x := x.(type) {
		case float64:
			return x * n, nil
		case int64:
			return int64(float64(x) * n), nil
		case nil:
			return x, nil
		default:
			return nil, fmt.Errorf("factor: invalid type %T", x)
		}
	}
}
