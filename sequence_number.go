package sgreplicate

import (
	"strconv"

	"github.com/couchbase/clog"
)

const EMPTY_SEQUENCE_NUMBER = 0

func SequenceNumberToString(sequence interface{}) string {

	clog.To("Replicate", "sequenceNumberToString called with: %v type: %T", sequence, sequence)
	if sequence, ok := sequence.(int); ok {
		clog.To("Replicate", "sequence is an int")
		return strconv.Itoa(sequence)
	}
	if sequence, ok := sequence.(float64); ok {
		clog.To("Replicate", "sequence is a float64")
		sequenceInt := int(sequence)
		return strconv.Itoa(sequenceInt)
	}

	if sequence, ok := sequence.(string); ok {
		clog.To("Replicate", "sequence is a string")
		return sequence
	}
	clog.Panicf("Unable to convert %v to string", sequence)
	return "error"

}
