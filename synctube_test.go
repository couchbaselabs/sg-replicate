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
	targetServer.Response(500, headers, "{\"bogus\": true}")

	params := ReplicationParameters{}
	params.Source = sourceServer.URL
	params.SourceDb = "db"
	params.Target = targetServer.URL
	params.TargetDb = "db"
	params.Continuous = false

	queueSize := 1
	notificationChan := make(chan ReplicationNotification, queueSize)

	// create a new replication and start it
	logg.LogTo("TEST", "Starting ..")
	replication := NewReplication(params, notificationChan)
	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	// since the attempt to get the checkpoint from the target
	// server will fail, expect to get a replication stopped event
	replicationNotification = <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_STOPPED)

	// the notification chan should be closed now
	logg.LogTo("TEST", "Checking notification channel closed ..")
	_, ok := <-notificationChan
	assert.False(t, ok)

}

func TestGetTargetCheckpoint(t *testing.T) {

	targetServer := fakehttp.NewHTTPServerWithPort(5986)

	params := ReplicationParameters{}
	params.Target = targetServer.URL
	replication := NewReplication(params, nil)
	targetChekpoint := replication.getTargetCheckpoint()
	logg.LogTo("TEST", "checkpoint: %v", targetChekpoint)

}

func TestOneShotReplicationBrokenChangesFeed(t *testing.T) {

	// startup fake source server
	sourceServer := fakehttp.NewHTTPServerWithPort(5987)
	sourceServer.Start()

	// startup fake target server
	targetServer := fakehttp.NewHTTPServerWithPort(5986)
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

	// create a new replication and start it
	logg.LogTo("TEST", "Starting ..")
	replication := NewReplication(params, notificationChan)
	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	// since the attempt to get the checkpoint from the target
	// server will fail, expect to get a replication stopped event
	replicationNotification = <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_STOPPED)

	// the notification chan should be closed now
	logg.LogTo("TEST", "Checking notification channel closed ..")
	_, ok := <-notificationChan
	assert.False(t, ok)

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
