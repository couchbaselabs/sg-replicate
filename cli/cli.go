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

	// TODO: if no continuous replications, should finish and not block

	doneChan := make(chan bool)
	startedContinuousReplications := false
	for _, replicationParams := range replicationsConfig.Replications {
		switch replicationParams.Lifecycle {
		case synctube.ONE_SHOT:
			err := runOneshotReplication(
				replicationsConfig,
				replicationParams,
			)
			if err != nil {
				logg.LogPanic("Unable to run replication: %v. Err: %v", replicationParams, err.Error())
			}
			logg.LogTo("CLI", "Successfully ran one shot replication: %v", replicationParams)
		case synctube.CONTINUOUS:
			startedContinuousReplications = true
			go launchContinuousReplication(
				replicationsConfig,
				replicationParams,
				doneChan,
			)
		}

	}

	// if we started any continuous replications, block until
	// any of them stop ( and they should never stop under normal
	// conditions)
	if startedContinuousReplications {

		<-doneChan

		// if any continuous replications die, just panic.
		logg.LogPanic("One or more replications stopped")
	}

}

func runOneshotReplication(config ReplicationsConfig, params synctube.ReplicationParameters) error {

	_, err := synctube.RunOneShotReplication(params)
	return err

}

func launchContinuousReplication(config ReplicationsConfig, params synctube.ReplicationParameters, doneChan chan bool) {

	notificationChan := make(chan synctube.ContinuousReplicationNotification)

	factory := func(params synctube.ReplicationParameters, notificationChan chan synctube.ReplicationNotification) synctube.Runnable {
		params.Lifecycle = synctube.ONE_SHOT
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
		}
	}

}
