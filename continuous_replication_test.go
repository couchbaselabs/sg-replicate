package synctube

import (
	"github.com/couchbaselabs/logg"
	"testing"
)

func init() {
	logg.LogKeys["TEST"] = true
	logg.LogKeys["SYNCTUBE"] = true
}

func TestContinuousReplication(t *testing.T) {

	logg.LogTo("TEST", "TestContinuousReplication")

	sourceServer, targetServer := fakeServers(5975, 5974)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ContinuousReplicationNotification)

	factory := func(params ReplicationParameters, notificationChan chan ReplicationNotification) Runnable {
		return NewReplication(params, notificationChan)

	}

	replication := NewContinuousReplication(params, factory, notificationChan)
	logg.LogTo("TEST", "created replication: %v", replication)

	for notification := range notificationChan {
		logg.LogTo("TEST", "got notification: %v", notification)
	}

}
