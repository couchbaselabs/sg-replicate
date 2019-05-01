package sgreplicate

import (
	"fmt"
	"strconv"
)

const EMPTY_SEQUENCE_NUMBER = 0

func SequenceNumberToString(sequence interface{}) string {
	if sequence, ok := sequence.(int); ok {
		return strconv.Itoa(sequence)
	}
	if sequence, ok := sequence.(float64); ok {
		sequenceInt := int(sequence)
		return strconv.Itoa(sequenceInt)
	}
	if sequence, ok := sequence.(string); ok {
		return sequence
	}
	return fmt.Sprintf("Unable to convert %v to string", sequence)

}
