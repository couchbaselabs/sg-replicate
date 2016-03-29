package sgreplicate

import (
	"strconv"

	"github.com/couchbaselabs/logg"
)

const EMPTY_SEQUENCE_NUMBER = 0

func SequenceNumberToString(sequence interface{}) string {

	logg.LogTo("Replicate", "sequenceNumberToString called with: %v type: %T", sequence, sequence)
	if sequence, ok := sequence.(int); ok {
		logg.LogTo("Replicate", "sequence is an int")
		return strconv.Itoa(sequence)
	}
	if sequence, ok := sequence.(float64); ok {
		logg.LogTo("Replicate", "sequence is a float64")
		sequenceInt := int(sequence)
		return strconv.Itoa(sequenceInt)
	}

	if sequence, ok := sequence.(string); ok {
		logg.LogTo("Replicate", "sequence is a string")
		return sequence
	}
	logg.LogPanic("Unable to convert %v to string", sequence)
	return "error"

}
