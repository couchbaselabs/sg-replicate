package main

import (
	"fmt"
	"github.com/couchbaselabs/logg"
	"github.com/tleyden/synctube"
	"net/url"
	"time"
)

func init() {
	logg.LogKeys["CLI"] = true
	logg.LogKeys["SYNCTUBE"] = true
}

func main() {

	fmt.Printf("hey")

	sourceServerUrlStr := "http://checkers.sync.couchbasecloud.com"
	targetServerUrlStr := sourceServerUrlStr

	sourceServerUrl, err := url.Parse(sourceServerUrlStr)
	if err != nil {
		logg.LogPanic("could not parse url: %v", sourceServerUrlStr)
	}

	targetServerUrl, err := url.Parse(targetServerUrlStr)
	if err != nil {
		logg.LogPanic("could not parse url: %v", targetServerUrlStr)
	}

	params := synctube.ReplicationParameters{}
	params.Source = sourceServerUrl
	params.SourceDb = "checkers"
	params.Target = targetServerUrl
	params.TargetDb = "checkers-copy"
	params.ChangesFeedLimit = 50

	notificationChan := make(chan synctube.ContinuousReplicationNotification)

	factory := func(params synctube.ReplicationParameters, notificationChan chan synctube.ReplicationNotification) synctube.Runnable {
		return synctube.NewReplication(params, notificationChan)
	}

	retryTime := time.Millisecond
	replication := synctube.NewContinuousReplication(params, factory, notificationChan, retryTime)
	logg.LogTo("TEST", "created continuous replication: %v", replication)

	for {
		select {
		case notification, ok := <-notificationChan:
			if !ok {
				logg.LogPanic("CLI", "notificationChan appears to be closed")
				return
			}
			logg.LogTo("CLI", "Got notification %v", notification)

		case <-time.After(time.Second * 120):
			logg.LogPanic("Timeout")
		}
	}

}
