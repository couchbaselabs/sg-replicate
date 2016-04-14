package sgreplicate

import (
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/couchbaselabs/go.assert"
	"github.com/couchbase/clog"
)

func init() {
	clog.EnableKey("TEST")
	clog.EnableKey("Replicate")
}

type MockOneShotReplication struct {
	NotificationChan chan ReplicationNotification

	// if true, it will stop.  if false, will abort
	stopWhenFinished bool

	// fake checkpoint sent in the REPLICATION_STOPPED notification
	fakeCheckpoint interface{}

	fakeStats ReplicationStats
}

func (r MockOneShotReplication) Start() error {

	clog.To("TEST", "MockOneShotReplicatin.Start() called. ")

	go r.pretendToBeAOneShotReplicator()

	return nil
}

func (r MockOneShotReplication) pretendToBeAOneShotReplicator() {

	clog.To("TEST", "%v pretendToBeAOneShotReplicator() called.", r)

	// <-time.After(2 * time.Second)

	if r.stopWhenFinished {
		clog.To("TEST", "send REPLICATION_STOPPED to %v", r.NotificationChan)

		notification := *(NewReplicationNotification(REPLICATION_STOPPED))
		r.fakeStats.EndLastSeq = r.fakeCheckpoint
		notification.Data = r.fakeStats
		r.NotificationChan <- notification

		clog.To("TEST", "sent REPLICATION_STOPPED")
	} else {
		clog.To("TEST", "send REPLICATION_ABORTED to %v", r.NotificationChan)

		r.NotificationChan <- *(NewReplicationNotification(REPLICATION_ABORTED))

		clog.To("TEST", "sent REPLICATION_ABORTED")

	}

}

func waitForContinuousNotification(notificationChan chan ContinuousReplicationNotification, expected ContinuousReplicationNotification) {

	clog.To("TEST", "Waiting for %v", expected)

	for {
		select {
		case notification, ok := <-notificationChan:
			if !ok {
				clog.Panic("TEST", "notificationChan appears to be closed")
				return
			}
			if notification == expected {
				clog.To("TEST", "Got %v", expected)
				return
			} else {
				clog.To("TEST", "Waiting for %v but got %v, igoring", expected, notification)
			}

		case <-time.After(time.Second * 10):
			clog.Panic("Timeout waiting for %v", expected)
		}
	}

}

// Test against mock servers which are already in sync
func TestNoOpContinuousReplication(t *testing.T) {

	sourceServer, targetServer := fakeServers(5971, 5970)

	// fake changes feed - empty
	lastSequence := "7"
	sourceServer.Response(200, jsonHeaders(), fakeChangesFeedEmpty(lastSequence))

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ContinuousReplicationNotification)

	factory := func(params ReplicationParameters, notificationChan chan ReplicationNotification) Runnable {

		return &MockOneShotReplication{
			NotificationChan: notificationChan,
			stopWhenFinished: true,
			fakeCheckpoint:   lastSequence,
		}

	}

	retryTime := time.Millisecond

	replication := NewContinuousReplication(params, factory, notificationChan, retryTime)

	waitForContinuousNotification(notificationChan, CATCHING_UP)
	waitForContinuousNotification(notificationChan, CAUGHT_UP)

	replication.Stop()
	waitForContinuousNotification(notificationChan, CANCELLED)

	for _, savedReq := range sourceServer.SavedRequests {
		path := savedReq.Request.URL.Path
		if strings.Contains(path, "/db/_changes") {
			params, err := url.ParseQuery(savedReq.Request.URL.RawQuery)
			assert.True(t, err == nil)
			assert.Equals(t, params["since"][0], lastSequence)
		}
	}

}

func TestHappyPathContinuousReplication(t *testing.T) {

	sourceServer, targetServer := fakeServers(5975, 5974)

	// fake response to changes feed
	lastSequence := "1"
	sourceServer.Response(200, jsonHeaders(), fakeChangesFeed(lastSequence))

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ContinuousReplicationNotification)

	factory := func(params ReplicationParameters, notificationChan chan ReplicationNotification) Runnable {

		return &MockOneShotReplication{
			NotificationChan: notificationChan,
			stopWhenFinished: true,
			fakeCheckpoint:   lastSequence,
		}

	}

	retryTime := time.Millisecond

	replication := NewContinuousReplication(params, factory, notificationChan, retryTime)

	waitForContinuousNotification(notificationChan, CATCHING_UP)
	waitForContinuousNotification(notificationChan, CAUGHT_UP)
	waitForContinuousNotification(notificationChan, CATCHING_UP)
	waitForContinuousNotification(notificationChan, CAUGHT_UP)

	replication.Stop()
	waitForContinuousNotification(notificationChan, CANCELLED)

	for _, savedReq := range sourceServer.SavedRequests {
		path := savedReq.Request.URL.Path
		if strings.Contains(path, "/db/_changes") {
			params, err := url.ParseQuery(savedReq.Request.URL.RawQuery)
			assert.True(t, err == nil)
			assert.Equals(t, params["since"][0], lastSequence)
		}
	}

}

// Test against a mock source server that emulates a wrapped replication
// that always aborts replications.  (NOTE: if we extend the continuous replication
// to abort when there are enough abort events from wrapped replication, we'll
// need to update this test)
func TestUnHealthyContinuousReplication(t *testing.T) {

	sourceServer, targetServer := fakeServers(5973, 5972)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ContinuousReplicationNotification)

	factory := func(params ReplicationParameters, notificationChan chan ReplicationNotification) Runnable {

		return &MockOneShotReplication{
			NotificationChan: notificationChan,
			stopWhenFinished: false,
			fakeCheckpoint:   1,
		}

	}
	retryTime := time.Millisecond
	replication := NewContinuousReplication(params, factory, notificationChan, retryTime)

	waitForContinuousNotification(notificationChan, CATCHING_UP)
	waitForContinuousNotification(notificationChan, ABORTED_WAITING_TO_RETRY)
	waitForContinuousNotification(notificationChan, CATCHING_UP)
	waitForContinuousNotification(notificationChan, ABORTED_WAITING_TO_RETRY)

	replication.Stop()
	waitForContinuousNotification(notificationChan, CANCELLED)

}

// Integration test.  Not automated yet, should be commented out.
func DISTestContinuousReplicationIntegration(t *testing.T) {

	sourceServerUrlStr := "http://localhost:4984"
	targetServerUrlStr := "http://localhost:4986"

	sourceServerUrl, err := url.Parse(sourceServerUrlStr)
	if err != nil {
		clog.Panic("could not parse url: %v", sourceServerUrlStr)
	}

	targetServerUrl, err := url.Parse(targetServerUrlStr)
	if err != nil {
		clog.Panic("could not parse url: %v", targetServerUrlStr)
	}
	params := replicationParams(sourceServerUrl, targetServerUrl)

	notificationChan := make(chan ContinuousReplicationNotification)

	factory := func(params ReplicationParameters, notificationChan chan ReplicationNotification) Runnable {
		return NewReplication(params, notificationChan)
	}

	retryTime := time.Millisecond
	replication := NewContinuousReplication(params, factory, notificationChan, retryTime)
	clog.To("TEST", "created continuous replication: %v", replication)

	for {
		select {
		case notification, ok := <-notificationChan:
			if !ok {
				clog.Panic("TEST", "notificationChan appears to be closed")
				return
			}
			clog.To("TEST", "Got notification %v", notification)

		case <-time.After(time.Second * 120):
			clog.Panic("Timeout")
		}
	}

}
