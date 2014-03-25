package synctube

import (
	"crypto"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/couchbaselabs/logg"
	"github.com/mreiferson/go-httpclient"
	"io/ioutil"
	"net/http"
	"time"
)

type Replication struct {
	Parameters              ReplicationParameters
	EventChan               chan ReplicationEvent
	NotificationChan        chan ReplicationNotification
	FetchedTargetCheckpoint Checkpoint
}

func NewReplication(params ReplicationParameters, notificationChan chan ReplicationNotification) *Replication {

	eventChan := make(chan ReplicationEvent)

	replication := &Replication{
		Parameters:       params,
		EventChan:        eventChan,
		NotificationChan: notificationChan,
	}

	// spawn a go-routine that reads from event channel and acts on events
	go replication.processEvents()

	return replication

}

// Start this replication
func (r *Replication) Start() {
	event := NewReplicationEvent(REPLICATION_START)
	r.EventChan <- *event
}

// Stop this replication
func (r *Replication) Stop() {
	event := NewReplicationEvent(REPLICATION_STOP)
	r.EventChan <- *event
}

func (r *Replication) processEvents() {

	defer close(r.EventChan)        // No more events
	defer close(r.NotificationChan) // No more notifications

	for state := stateFnPreStarted; state != nil; {
		state = state(r)
	}

	logg.LogTo("SYNC", "processEvents() is done")

}

func (r Replication) getTargetCheckpoint() string {
	targetUrlString := r.Parameters.getTargetDbUrl()
	hash := crypto.SHA1.New()
	hash.Sum([]byte(targetUrlString))
	return hex.EncodeToString(hash.Sum(nil))
}

func (r Replication) fetchTargetCheckpoint() {

	checkpoint := r.getTargetCheckpoint()
	destUrl := fmt.Sprintf("%s/_local/%s", r.Parameters.getTargetDbUrl(), checkpoint)

	transport := r.getTransport()
	defer transport.Close()

	client := &http.Client{Transport: transport}
	req, _ := http.NewRequest("GET", destUrl, nil)
	resp, err := client.Do(req)
	logg.LogTo("SYNCTUBE", "resp: %v, err: %v", resp, err)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error getting checkpoint: %v", err)
		event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
		r.EventChan <- *event
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		// valid response, continue with empty remote checkpoint
		logg.LogTo("SYNCTUBE", "404 trying to get checkpoint, continue..")
		event := NewReplicationEvent(FETCH_CHECKPOINT_SUCCEEDED)
		checkpoint := Checkpoint{LastSequence: "0"}
		event.Data = checkpoint.LastSequence
		r.EventChan <- *event
	} else if resp.StatusCode >= 400 {
		// we got an error, lets abort
		logg.LogTo("SYNCTUBE", "4xx error(not 404) getting checkpoint")
		event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
		r.EventChan <- *event
	} else if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// looks like we got a valid checkpoint
		logg.LogTo("SYNCTUBE", "valid checkpoint")
		event := NewReplicationEvent(FETCH_CHECKPOINT_SUCCEEDED)
		bodyText, _ := ioutil.ReadAll(resp.Body)
		logg.LogTo("SYNCTUBE", "body: %v", string(bodyText))
		checkpoint := Checkpoint{}
		err = json.Unmarshal(bodyText, &checkpoint)
		if err != nil {
			logg.LogPanic("Error unmarshalling checkpoint %v:", err)
		}
		logg.LogTo("SYNCTUBE", "checkpoint: %v", checkpoint.LastSequence)
		event.Data = checkpoint.LastSequence
		logg.LogTo("SYNCTUBE", "event: %v", event)
		r.EventChan <- *event

	} else {
		// unexpected http status, abort
		logg.LogTo("SYNCTUBE", "unexpected http status %v", resp.StatusCode)
		event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
		r.EventChan <- *event
	}

}

func (r Replication) getChangesFeedUrl() string {
	// TODO: add since param

	dbUrl := r.Parameters.getSourceDbUrl()
	return fmt.Sprintf(
		"%s/_changes?feed=%s&limit=%s&heartbeat=%s&style=%s",
		dbUrl,
		r.Parameters.getChangesFeedType(),
		r.Parameters.getChangesFeedLimit(),
		r.Parameters.getChangesFeedHeartbeat(),
		r.Parameters.getChangesFeedStyle())

}

func (r Replication) getTransport() *httpclient.Transport {
	return &httpclient.Transport{
		ConnectTimeout:        60 * time.Second,
		RequestTimeout:        60 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
	}
}

func (r *Replication) fetchChangesFeed() {

	destUrl := r.getChangesFeedUrl()

	transport := r.getTransport()
	defer transport.Close()

	client := &http.Client{Transport: transport}
	req, _ := http.NewRequest("GET", destUrl, nil)
	resp, err := client.Do(req)
	logg.LogTo("SYNCTUBE", "changes feed resp: %v, err: %v", resp, err)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error getting changes feed: %v", err)
		event := NewReplicationEvent(FETCH_CHANGES_FEED_FAILED)
		r.EventChan <- *event
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		logg.LogTo("SYNCTUBE", "Error getting changes feed.  Resp: %v", resp)
		event := NewReplicationEvent(FETCH_CHANGES_FEED_FAILED)
		r.EventChan <- *event
	} else {

	}

}
