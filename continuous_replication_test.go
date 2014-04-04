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
}

func (r MockOneShotReplication) Start() error {

	go r.pretendToBeAOneShotReplicator()

	return nil
}

func (r MockOneShotReplication) pretendToBeAOneShotReplicator() {

	logg.LogTo("TEST", "wait 2 seconds")

	<-time.After(2 * time.Second)

	logg.LogTo("TEST", "send REPLICATION_ABORTED to %v", r.NotificationChan)

	r.NotificationChan <- *(NewReplicationNotification(REPLICATION_ABORTED))

	logg.LogTo("TEST", "sent REPLICATION_ABORTED")

	<-time.After(5 * time.Second)

	logg.LogTo("TEST", "send REPLICATION_STOPPED")

	r.NotificationChan <- *(NewReplicationNotification(REPLICATION_STOPPED))

	<-time.After(25 * time.Second)

	r.NotificationChan <- *(NewReplicationNotification(REPLICATION_STOPPED))

}

func TestContinuousReplication(t *testing.T) {

	logg.LogTo("TEST", "TestContinuousReplication")

	sourceServer, targetServer := fakeServers(5975, 5974)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ContinuousReplicationNotification)

	factory := func(params ReplicationParameters, notificationChan chan ReplicationNotification) Runnable {

		return &MockOneShotReplication{
			NotificationChan: notificationChan,
		}

	}

	replication := NewContinuousReplication(params, factory, notificationChan)
	logg.LogTo("TEST", "created replication: %v", replication)

	for notification := range notificationChan {
		logg.LogTo("TEST", "got notification: %v", notification)
	}

}
