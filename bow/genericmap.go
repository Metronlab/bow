package bow

import (
	"github.com/apache/arrow/go/arrow"
	"time"
)

type GenericMap map[string]interface{}

func (g GenericMap) getFloat64Value(key string) (value float64, valid bool) {
	value, valid = g[key].(float64)
	return
}

func (g GenericMap) getInt64Value(key string) (value int64, valid bool) {
	value, valid = g[key].(int64)
	return
}

func (g GenericMap) getTimeMsValue(key string) (value arrow.Time32, valid bool) {
	v, valid := g[key].(time.Time)
	if !valid {
		return
	}
	return arrow.Time32(v.UnixNano() / 1e6), valid
}

func (g GenericMap) getBoolValue(key string) (value bool, valid bool) {
	value, valid = g[key].(bool)
	return
}