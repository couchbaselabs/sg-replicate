package main

import (
	"bufio"
	"os"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/couchbase/clog"
	sgreplicate "github.com/couchbaselabs/sg-replicate"
)

var (
	configFileDescription = "The name of the config file.  Defaults to 'config.json'"
	configFileName        = kingpin.Arg("config file name", configFileDescription).Default("config.json").String()
)

func init() {
	clog.EnableKey("CLI")
	clog.EnableKey("Replicate")
}

func main() {

	kingpin.Parse()
	if *configFileName == "" {
		kingpin.Errorf("Config file name missing")
		return
	}
	configFile, err := os.Open(*configFileName)
	if err != nil {
		clog.Panicf("Unable to open file: %v.  Err: %v", *configFileName, err.Error())
		return
	}
	defer configFile.Close()

	configReader := bufio.NewReader(configFile)
	replicationsConfig, err := ParseReplicationsConfig(configReader)
	if err != nil {
		clog.Panicf("Unable to parse config: %v. Err: %v", *configFileName, err.Error())
		return
	}

	launchReplications(replicationsConfig)

}

func launchReplications(replicationsConfig ReplicationsConfig) {

	// TODO: if no continuous replications, should finish and not block

	doneChan := make(chan bool)
	startedContinuousReplications := false
	for _, replicationParams := range replicationsConfig.Replications {
		if replicationParams.Disabled {
			clog.To("CLI", "Skip disabled replication: %v", replicationParams)
			continue
		}
		switch replicationParams.Lifecycle {
		case sgreplicate.ONE_SHOT:
			err := runOneshotReplication(
				replicationsConfig,
				replicationParams,
			)
			if err != nil {
				clog.Panicf("Unable to run replication: %v. Err: %v", replicationParams, err.Error())
			}
			clog.To("CLI", "Successfully ran one shot replication: %v", replicationParams)
		case sgreplicate.CONTINUOUS:
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
		clog.Panicf("One or more replications stopped")
	}

}

func runOneshotReplication(config ReplicationsConfig, params sgreplicate.ReplicationParameters) error {

	_, err := sgreplicate.RunOneShotReplication(params)
	return err

}

func launchContinuousReplication(config ReplicationsConfig, params sgreplicate.ReplicationParameters, doneChan chan bool) {

	notificationChan := make(chan sgreplicate.ContinuousReplicationNotification)

	factory := func(params sgreplicate.ReplicationParameters, notificationChan chan sgreplicate.ReplicationNotification) sgreplicate.Runnable {
		params.Lifecycle = sgreplicate.ONE_SHOT
		return sgreplicate.NewReplication(params, notificationChan)
	}

	retryTime := time.Millisecond * time.Duration(config.ContinuousRetryTimeMs)
	replication := sgreplicate.NewContinuousReplication(params, factory, notificationChan, retryTime)
	clog.To("TEST", "created continuous replication: %v", replication)

	for {
		select {
		case notification, ok := <-notificationChan:
			if !ok {
				clog.To("CLI", "%v notificationChan appears to be closed", replication)
				doneChan <- true
				return
			}
			clog.To("CLI", "Got notification %v", notification)
		}
	}

}
