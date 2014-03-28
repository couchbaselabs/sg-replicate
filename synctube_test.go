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

func TestOneShotReplicationGetCheckpointFailed(t *testing.T) {

	// the simulated sync gateway source only returns a _local doc
	// with a checkpoint.  after that, the request to the _changes
	// feed returns invalid responses and the replication
	// stops and goes into an error state

	sourceServer, targetServer := fakeServers(5985, 5984)

	// setup fake response on target server
	targetServer.Response(500, jsonHeaders(), bogusJson())

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	replication := NewReplication(params, notificationChan)
	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	// since the attempt to get the checkpoint from the target
	// server will fail, expect to get a replication stopped event
	replicationNotification = <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_STOPPED)

	assertNotificationChannelClosed(notificationChan)

}

func TestOneShotReplicationGetCheckpointHappypath(t *testing.T) {

	sourceServer, targetServer := fakeServers(5989, 5988)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	replication := NewReplication(params, notificationChan)

	lastSequence := 1
	jsonResponse := fakeCheckpointResponse(replication.targetCheckpointAddress(), 1)
	targetServer.Response(200, jsonHeaders(), jsonResponse)

	replication.Start()

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	// get the fetched checkpoint and make sure it matches
	lastSequenceFetched, err := replication.FetchedTargetCheckpoint.LastCheckpointNumeric()
	assert.True(t, err == nil)
	assert.Equals(t, lastSequence, lastSequenceFetched)

	replication.Stop()
	waitForNotification(replication, REPLICATION_STOPPED)

	assertNotificationChannelClosed(notificationChan)

}

func TestOneShotReplicationGetChangesFeedFailed(t *testing.T) {

	sourceServer, targetServer := fakeServers(5987, 5986)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	replication := NewReplication(params, notificationChan)

	targetServer.Response(200, jsonHeaders(), fakeCheckpointResponse(replication.targetCheckpointAddress(), 1))

	// fake response to changes feed
	sourceServer.Response(500, jsonHeaders(), "{\"error\": true}")

	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	waitForNotification(replication, REPLICATION_STOPPED)

	assertNotificationChannelClosed(notificationChan)

}

func TestOneShotReplicationGetChangesFeedHappyPath(t *testing.T) {

	sourceServer, targetServer := fakeServers(5991, 5990)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	replication := NewReplication(params, notificationChan)

	targetServer.Response(200, jsonHeaders(), fakeCheckpointResponse(replication.targetCheckpointAddress(), 1))

	// response to changes feed
	fakeChangesFeed := fakeChangesFeed()
	sourceServer.Response(200, jsonHeaders(), fakeChangesFeed)

	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	waitForNotification(replication, REPLICATION_FETCHED_CHANGES_FEED)

	changes := replication.Changes
	assert.Equals(t, len(changes.Results), 2)

	replication.Stop()

	waitForNotification(replication, REPLICATION_STOPPED)

	assertNotificationChannelClosed(notificationChan)

}

func TestOneShotReplicationGetChangesFeedEmpty(t *testing.T) {

	sourceServer, targetServer := fakeServers(5993, 5992)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	replication := NewReplication(params, notificationChan)

	targetServer.Response(200, jsonHeaders(), fakeCheckpointResponse(replication.targetCheckpointAddress(), 1))

	// response to changes feed
	fakeChangesFeed := fakeEmptyChangesFeed()
	sourceServer.Response(200, jsonHeaders(), fakeChangesFeed)

	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	waitForNotification(replication, REPLICATION_FETCHED_CHANGES_FEED)

	changes := replication.Changes
	assert.Equals(t, len(changes.Results), 0)

	waitForNotification(replication, REPLICATION_STOPPED)

	assertNotificationChannelClosed(notificationChan)

}

func TestOneShotReplicationGetRevsDiffFailed(t *testing.T) {

	sourceServer, targetServer := fakeServers(5995, 5994)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	// create a new replication and start it
	replication := NewReplication(params, notificationChan)

	targetServer.Response(200, jsonHeaders(), fakeCheckpointResponse(replication.targetCheckpointAddress(), 1))

	// fake response to changes feed
	sourceServer.Response(200, jsonHeaders(), fakeChangesFeed())

	// fake response to revs_diff
	targetServer.Response(500, jsonHeaders(), "{\"error\": true}")

	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	waitForNotification(replication, REPLICATION_STOPPED)

	assertNotificationChannelClosed(notificationChan)

}

func TestOneShotReplicationGetRevsDiffHappyPath(t *testing.T) {

	sourceServer, targetServer := fakeServers(5997, 5996)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	// create a new replication and start it
	replication := NewReplication(params, notificationChan)

	targetServer.Response(200, jsonHeaders(), fakeCheckpointResponse(replication.targetCheckpointAddress(), 1))

	// fake response to changes feed
	sourceServer.Response(200, jsonHeaders(), fakeChangesFeed())

	// fake response to revs_diff
	targetServer.Response(200, jsonHeaders(), fakeRevsDiff())

	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	waitForNotification(replication, REPLICATION_FETCHED_REVS_DIFF)

	replication.Stop()

	waitForNotification(replication, REPLICATION_STOPPED)

	assertNotificationChannelClosed(notificationChan)

}

func TestOneShotReplicationBulkGetFailed(t *testing.T) {

	sourceServer, targetServer := fakeServers(5999, 5998)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	// create a new replication and start it
	replication := NewReplication(params, notificationChan)

	targetServer.Response(200, jsonHeaders(), fakeCheckpointResponse(replication.targetCheckpointAddress(), 1))

	// fake response to changes feed
	sourceServer.Response(200, jsonHeaders(), fakeChangesFeed())

	// fake response to bulk get
	sourceServer.Response(500, jsonHeaders(), bogusJson())

	// fake response to revs_diff
	targetServer.Response(200, jsonHeaders(), fakeRevsDiff())

	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	waitForNotification(replication, REPLICATION_FETCHED_REVS_DIFF)

	waitForNotification(replication, REPLICATION_STOPPED)

	assertNotificationChannelClosed(notificationChan)

}

func fakeCheckpointResponse(checkpointAddress string, lastSequence int) string {
	return fmt.Sprintf(`{"id":"_local/%s","ok":true,"rev":"0-2","lastSequence":"%v"}`, checkpointAddress, lastSequence)

}

func jsonHeaders() map[string]string {
	return map[string]string{"Content-Type": "application/json"}
}

func assertNotificationChannelClosed(notificationChan chan ReplicationNotification) {
	_, ok := <-notificationChan
	if ok {
		logg.LogPanic("notificationChan was not closed")
	}
}

func fakeChangesFeed() string {
	return `{"results":[{"seq":2,"id":"doc2","changes":[{"rev":"1-5e38"}]},{"seq":3,"id":"doc3","changes":[{"rev":"1-563b"}]}],"last_seq":3}`
}

func fakeRevsDiff() string {
	return `{"doc2":{"missing":["1-5e38"]}}`
}

func fakeEmptyChangesFeed() string {
	return `{"results":[],"last_seq":3}`
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

func bogusJson() string {
	return `{"bogus": true}`
}

func TestGetTargetCheckpoint(t *testing.T) {

	targetServer := fakehttp.NewHTTPServerWithPort(5986)

	params := ReplicationParameters{}
	params.Target = targetServer.URL
	replication := NewReplication(params, nil)
	targetCheckpoint := replication.targetCheckpointAddress()
	logg.LogTo("TEST", "checkpoint: %v", targetCheckpoint)

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
