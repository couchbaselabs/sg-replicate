package main

import (
	"bufio"
	"github.com/couchbaselabs/logg"
	"github.com/tleyden/synctube"
	"os"
	"time"
)

func init() {
	logg.LogKeys["CLI"] = true
	logg.LogKeys["SYNCTUBE"] = true
}

func main() {

	filename := "config.json"
	configFile, err := os.Open(filename)
	if err != nil {
		logg.LogPanic("Unable to open file: %v.  Err: %v", filename, err.Error())
		return
	}
	defer configFile.Close()

	configReader := bufio.NewReader(configFile)
	replicationsConfig, err := ParseReplicationsConfig(configReader)
	if err != nil {
		logg.LogPanic("Unable to parse config: %v. Err: %v", filename, err.Error())
		return
	}

	launchReplications(replicationsConfig)

}

func launchReplications(replicationsConfig ReplicationsConfig) {

	doneChan := make(chan bool)
	for _, replicationParams := range replicationsConfig.Replications {
		// TODO: here is where we could differentiate between one
		// shot and continuous replications
		go launchContinuousReplication(replicationsConfig, replicationParams, doneChan)
	}

	<-doneChan

	// for now, if any continuous replications die, just panic.
	logg.LogPanic("One or more replications stopped")

}

func launchContinuousReplication(config ReplicationsConfig, params synctube.ReplicationParameters, doneChan chan bool) {

	notificationChan := make(chan synctube.ContinuousReplicationNotification)

	factory := func(params synctube.ReplicationParameters, notificationChan chan synctube.ReplicationNotification) synctube.Runnable {
		return synctube.NewReplication(params, notificationChan)
	}

	retryTime := time.Millisecond * time.Duration(config.ContinuousRetryTimeMs)
	replication := synctube.NewContinuousReplication(params, factory, notificationChan, retryTime)
	logg.LogTo("TEST", "created continuous replication: %v", replication)

	for {
		select {
		case notification, ok := <-notificationChan:
			if !ok {
				logg.LogTo("CLI", "%v notificationChan appears to be closed", replication)
				doneChan <- true
				return
			}
			logg.LogTo("CLI", "Got notification %v", notification)

		case <-time.After(time.Second * 120):
			logg.LogTo("CLI", "Timeout waiting for notification from %v", replication)
			doneChan <- true

		}
	}

}
