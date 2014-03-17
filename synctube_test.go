package synctube

import (
	"github.com/couchbaselabs/go.assert"
	"testing"
)

func TestOneShotReplication(t *testing.T) {

	// TODO: setup fakehttp

	params := ReplicationParameters{}
	params.Source = "foo.com"
	params.Target = "bar.com"
	params.Continuous = true

	eventChan := make(chan ReplicationEvent)

	replication := NewReplication(params, eventChan)
	replication.Start()

	sawReplicationStart := false
	sawReplicationFinish := false
	for replicationEvent := range eventChan {
		switch replicationEvent.Status() {
		case ReplicationEvent.STATUS_ACTIVE:
			sawReplicationStart = true
		case ReplicationEvent.STATUS_STOPPED:
			sawReplicationStart = true
			// TODO: assertions about num documents transferred
		}
	}

	assert.True(t, sawReplicationStart)
	assert.True(t, sawReplicationFinish)

}

func TestContinuousReplication(t *testing.T) {

}
