package arrow

import (
	"git.prod.metronlab.io/replica_set/timeserenity/client"
	"github.com/apache/arrow/go/arrow/array"
)

type Bow interface {

}

//NewBowFromObjs return Bow associated to an array of objects
func NewBowFromObjs(objs interface{}) (Bow, error) {
	panic("implement me")
}

//NewBowFromObjs return Bow associated to an array of objects
func NewBowFromMergeTimeIndex(objs interface{}) (Bow, error) {
	panic("implement me")
}

// influx result is an array of interfaces,
// fieldIdentifierOverride give explicit names to Bow generated schema.
func NewBowFromInfluxV2(result influx.Result, fieldIdentifierOverride map[int]string) (Bow, error) {
	panic("implement me")
}

type bow struct {
	record array.Record
}