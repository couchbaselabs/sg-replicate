package synctube

import (
	"github.com/couchbaselabs/logg"
	"testing"
	"time"
)

func init() {
	logg.LogKeys["TEST"] = true
	logg.LogKeys["SYNCTUBE"] = true
}

type MockOneShotReplication struct {
	NotificationChan chan ReplicationNotification

	// if true, it will stop.  if false, will abort
	stopWhenFinished bool
}

func (r MockOneShotReplication) Start() error {

	logg.LogTo("TEST", "MockOneShotReplicatin.Start() called. ")

	go r.pretendToBeAOneShotReplicator()

	return nil
}

func (r MockOneShotReplication) pretendToBeAOneShotReplicator() {

	logg.LogTo("TEST", "%v pretendToBeAOneShotReplicator() called.", r)

	// <-time.After(2 * time.Second)

	if r.stopWhenFinished {
		logg.LogTo("TEST", "send REPLICATION_STOPPED to %v", r.NotificationChan)

		r.NotificationChan <- *(NewReplicationNotification(REPLICATION_STOPPED))

		logg.LogTo("TEST", "sent REPLICATION_STOPPED")
	} else {
		logg.LogTo("TEST", "send REPLICATION_ABORTED to %v", r.NotificationChan)

		r.NotificationChan <- *(NewReplicationNotification(REPLICATION_ABORTED))

		logg.LogTo("TEST", "sent REPLICATION_ABORTED")

	}

}

func waitForContinuousNotification(notificationChan chan ContinuousReplicationNotification, expected ContinuousReplicationNotification) {

	logg.LogTo("TEST", "Waiting for %v", expected)

	for {
		select {
		case notification, ok := <-notificationChan:
			if !ok {
				logg.LogPanic("TEST", "notificationChan appears to be closed")
				return
			}
			if notification == expected {
				logg.LogTo("TEST", "Got %v", expected)
				return
			} else {
				logg.LogTo("TEST", "Waiting for %v but got %v, igoring", expected, notification)
			}

		case <-time.After(time.Second * 10):
			logg.LogPanic("Timeout waiting for %v", expected)
		}
	}

}

func TestHealthyContinuousReplication(t *testing.T) {

	logg.LogTo("TEST", "TestContinuousReplication")

	sourceServer, targetServer := fakeServers(5975, 5974)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ContinuousReplicationNotification)

	factory := func(params ReplicationParameters, notificationChan chan ReplicationNotification) Runnable {

		logg.LogTo("TEST", "Creating new MockOneShotReplication")
		return &MockOneShotReplication{
			NotificationChan: notificationChan,
			stopWhenFinished: true,
		}

	}

	replication := NewContinuousReplication(params, factory, notificationChan)
	logg.LogTo("TEST", "created replication: %v", replication)

	waitForContinuousNotification(notificationChan, CATCHING_UP)
	waitForContinuousNotification(notificationChan, CAUGHT_UP)
	waitForContinuousNotification(notificationChan, CATCHING_UP)
	waitForContinuousNotification(notificationChan, CAUGHT_UP)

	replication.Stop()
	waitForContinuousNotification(notificationChan, CANCELLED)

	logg.LogTo("TEST", "replication done: %v", replication)

}
