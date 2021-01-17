package mr

import (
	"strconv"
	"strings"
	"testing"
	"unicode"
)
func Map(filename string, contents string) []KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []KeyValue{}
	for _, w := range words {
		kv := KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}
func Reduce(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values))
}

//func TestDoMapTask(t *testing.T) {
//	NReduce = 1
//	for id := 0; id < mMap; id++ {
//		doMapTask(&Task{Phase: MapPhase, Id: id}, Map)
//	}
//}

func TestDoReduceTask(t *testing.T) {
	nReduce = 1
	for id := 0; id < nReduce; id++ {
		doReduceTask(&Task{Phase: ReducePhase, Id: id}, Reduce)
	}
}
