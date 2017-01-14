package sgreplicate

import (
	"testing"

	assert "github.com/couchbaselabs/go.assert"
)

func TestFilterRemovedChanges(t *testing.T) {

	changes := Changes{}
	changes.LastSequence = "whatever"
	changeNotRemoved := Change{}
	changeRemoved := Change{
		Removed: []string{"channel1"},
	}
	changes.Results = []Change{
		changeNotRemoved,
		changeRemoved,
	}
	newChanges := filterRemovedChanges(changes)
	assert.Equals(t, len(newChanges.Results), 1)

}
