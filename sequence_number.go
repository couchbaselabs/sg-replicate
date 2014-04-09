package synctube

import (
	"fmt"
)

const EMPTY_SEQUENCE_NUMBER = -1

type sequenceNumber int

func (s sequenceNumber) String() string {
	switch s {
	case EMPTY_SEQUENCE_NUMBER:
		return ""
	default:
		intVal := int(s)
		return fmt.Sprintf("%v", intVal)
	}

}
