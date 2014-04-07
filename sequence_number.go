package synctube

import (
	"fmt"
)

const EMPTY_SEQUENCE_NUMBER = -1

type sequenceNumber struct {
	wrappedInt int
}

func NewSequenceNumber(seqNumber int) *sequenceNumber {
	return &sequenceNumber{
		wrappedInt: seqNumber,
	}
}

func (s sequenceNumber) String() string {
	switch s.wrappedInt {
	case EMPTY_SEQUENCE_NUMBER:
		return ""
	default:
		return fmt.Sprintf("%v", s.wrappedInt)
	}

}
