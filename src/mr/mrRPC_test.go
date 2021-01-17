package mr

import (
	"strconv"
	"strings"
	"testing"
	"time"
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
func TestRPC(t *testing.T) {
	files := []string{"pg-dorian_gray.txt"}
	MakeMaster(files, 10)
	time.Sleep(5 * time.Second)
	Worker(Map, Reduce)
}