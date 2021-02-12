package bow

import "testing"

func BenchmarkJSON(b *testing.B) {
	series := []Series{
		NewSeries("int64", Int64, []int64{0, 1, 2}, []bool{true, true, false}),
		NewSeries("float64", Float64, []float64{0., 1., 2.}, []bool{true, true, false}),
		NewSeries("bool", Bool, []bool{true, false, false}, []bool{true, true, false}),
		NewSeries("string", String, []string{"toto", "tata", "titi"}, []bool{true, true, false}),
	}

	for _, s := range series {
		b.Run(s.Name, func(b *testing.B) {
			data, err := NewBow(s)
			if err != nil {
				panic(err)
			}

			data.SetMarshalJSONRowBased(true)
			var j []byte

			b.Run("marshallJSON", func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					j, err = data.MarshalJSON()
					if err != nil {
						panic(err)
					}
				}
			})

			b.Run("unmarshallJSON", func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					err := NewBowEmpty().UnmarshalJSON(j)
					if err != nil {
						panic(err)
					}
				}
			})
		})
	}
}
