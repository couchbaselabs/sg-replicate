package synctube

import (
	"fmt"
	"github.com/couchbaselabs/go.assert"
	"github.com/couchbaselabs/logg"
	"github.com/tleyden/fakehttp"
	"net/url"
	"testing"
	"time"
)

func init() {
	logg.LogKeys["TEST"] = true
	logg.LogKeys["SYNCTUBE"] = true
}

func replicationParams(sourceServerUrl *url.URL, targetServerUrl *url.URL) ReplicationParameters {
	params := ReplicationParameters{}
	params.Source = sourceServerUrl
	params.SourceDb = "db"
	params.Target = targetServerUrl
	params.TargetDb = "db"
	params.Continuous = false
	return params
}

func fakeServers(sourcePort, targetPort int) (source *fakehttp.HTTPServer, target *fakehttp.HTTPServer) {

	source = fakehttp.NewHTTPServerWithPort(sourcePort)
	source.Start()

	target = fakehttp.NewHTTPServerWithPort(targetPort)
	target.Start()

	return
}

func TestOneShotReplicationBrokenLocalDoc(t *testing.T) {

	// the simulated sync gateway source only returns a _local doc
	// with a checkpoint.  after that, the request to the _changes
	// feed returns invalid responses and the replication
	// stops and goes into an error state

	sourceServer, targetServer := fakeServers(5985, 5984)

	// setup fake response on target server
	headers := map[string]string{"Content-Type": "application/json"}
	targetServer.Response(500, headers, "{\"bogus\": true}")

	params := replicationParams(sourceServer.URL, targetServer.URL)

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

func TestOneShotReplicationGetCheckpointHappypath(t *testing.T) {

	sourceServer, targetServer := fakeServers(5989, 5988)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	queueSize := 1
	notificationChan := make(chan ReplicationNotification, queueSize)

	// create a new replication
	replication := NewReplication(params, notificationChan)

	lastSequence := "1"

	// setup fake response on target server
	headers := map[string]string{"Content-Type": "application/json"}
	jsonResponse := fmt.Sprintf("{\"id\":\"_local/%s\",\"ok\":true,\"rev\":\"0-2\",\"lastSequence\":\"%s\"}", replication.getTargetCheckpoint(), lastSequence)
	targetServer.Response(200, headers, jsonResponse)

	// start it
	logg.LogTo("TEST", "Starting ..")
	replication.Start()

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	// get the fetched checkpoint and make sure it matches
	assert.Equals(t, lastSequence, replication.FetchedTargetCheckpoint.LastSequence)

	replication.Stop()
	waitForNotification(replication, REPLICATION_STOPPED)

	// the notification chan should be closed now
	logg.LogTo("TEST", "Checking notification channel closed ..")
	_, ok := <-notificationChan
	assert.False(t, ok)

}

func TestOneShotReplicationBrokenChangesFeed(t *testing.T) {

	sourceServer, targetServer := fakeServers(5987, 5986)

	// setup fake response on target server
	headers := map[string]string{"Content-Type": "application/json"}
	targetServer.Response(200, headers, "{\"bogus\": true}")

	// fake response to changes feed
	sourceServer.Response(500, headers, "{\"error\": true}")

	params := replicationParams(sourceServer.URL, targetServer.URL)

	queueSize := 1
	notificationChan := make(chan ReplicationNotification, queueSize)

	// create a new replication and start it
	logg.LogTo("TEST", "Starting ..")
	replication := NewReplication(params, notificationChan)
	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	waitForNotification(replication, REPLICATION_STOPPED)

	// the notification chan should be closed now
	logg.LogTo("TEST", "Checking notification channel closed ..")
	_, ok := <-notificationChan
	assert.False(t, ok)

}

func waitForNotification(replication *Replication, expected ReplicationStatus) {
	logg.LogTo("TEST", "Waiting for %v", expected)
	notificationChan := replication.NotificationChan

	for {
		select {
		case replicationNotification := <-notificationChan:
			if replicationNotification.Status == expected {
				logg.LogTo("TEST", "Got %v", expected)
				return
			} else {
				logg.LogTo("TEST", "Waiting for %v but got %v, igoring", expected, replicationNotification.Status)
			}
		case <-time.After(time.Second * 10):
			logg.LogPanic("Timeout waiting for %v", expected)
		}
	}

}

func TestGetTargetCheckpoint(t *testing.T) {

	targetServer := fakehttp.NewHTTPServerWithPort(5986)

	params := ReplicationParameters{}
	params.Target = targetServer.URL
	replication := NewReplication(params, nil)
	targetChekpoint := replication.getTargetCheckpoint()
	logg.LogTo("TEST", "checkpoint: %v", targetChekpoint)

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
