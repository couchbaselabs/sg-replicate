package synctube

import (
	"github.com/couchbaselabs/go.assert"
	"github.com/couchbaselabs/logg"
	"github.com/tleyden/fakehttp"
	"testing"
)

func init() {
	logg.LogKeys["TEST"] = true
	logg.LogKeys["SYNCTUBE"] = true
}

func TestOneShotReplicationBrokenLocalDoc(t *testing.T) {

	// the simulated sync gateway source only returns a _local doc
	// with a checkpoint.  after that, the request to the _changes
	// feed returns invalid responses and the replication
	// stops and goes into an error state

	// startup fake source server
	sourceServer := fakehttp.NewHTTPServerWithPort(5985)
	sourceServer.Start()

	// startup fake target server
	targetServer := fakehttp.NewHTTPServerWithPort(5984)
	targetServer.Start()

	// setup fake response on target server
	headers := map[string]string{"Content-Type": "application/json"}
	targetServer.Response(200, headers, "{\"bogus\": true}")

	params := ReplicationParameters{}
	params.Source = sourceServer.URL
	params.Target = targetServer.URL
	params.Continuous = false

	queueSize := 1
	notificationChan := make(chan ReplicationNotification, queueSize)

	replication := NewReplication(params, notificationChan)
	replication.Start()

	sawReplicationActive := false
	logg.LogTo("TEST", "Looping over notificationChan events..")
	for replicationEvent := range notificationChan {
		logg.LogTo("TEST", "Got event: %v", replicationEvent)
		switch replicationEvent.Status {
		case REPLICATION_ACTIVE:
			sawReplicationActive = true
		}
	}

	logg.LogTo("TEST", "Looping over notificationChan finished")

	assert.True(t, sawReplicationActive)

}

func TestOneShotHappyPathReplication(t *testing.T) {

	// Happy Path test -- both the simulated source and targets
	// do exactly what is expected of them.  Make sure One Shot replication
	// completes.

	/*
		params := ReplicationParameters{}
		params.Source = "foo.com"
		params.Target = "bar.com"
		params.Continuous = true

		notificationChan := make(chan ReplicationEvent)

		replication := NewReplication(params, notificationChan)
		replication.Start()

		sawReplicationActive := false
		sawReplicationStopped := false
		for replicationEvent := range notificationChan {
			switch replicationEvent.Status() {
			case ReplicationEvent.STATUS_ACTIVE:
				sawReplicationActive = true
			case ReplicationEvent.STATUS_STOPPED:
				sawReplicationStopped = true
				// TODO: assertions about num documents transferred
			}
		}

		assert.True(t, sawReplicationActive)
		assert.True(t, sawReplicationStopped)
	*/

}

func TestContinuousHappyPathReplication(t *testing.T) {

	// Happy Path test -- both the simulated source and targets
	// do exactly what is expected of them.  Make sure Continous replication
	// completes.

}
