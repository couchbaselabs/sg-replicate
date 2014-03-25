package synctube

import (
	"crypto"
	"encoding/hex"
	"fmt"
	"github.com/couchbaselabs/logg"
	"github.com/mreiferson/go-httpclient"
	"net/http"
	"time"
)

type ReplicationNotification struct {
	Status ReplicationStatus
	// could have other stuff associated w/ notification
}

func NewReplicationNotification(status ReplicationStatus) *ReplicationNotification {
	return &ReplicationNotification{
		Status: status,
	}
}

type Replication struct {
	Parameters       ReplicationParameters
	EventChan        chan ReplicationEvent
	NotificationChan chan ReplicationNotification
}

type ReplicationStatus int

const (
	REPLICATION_STOPPED = ReplicationStatus(iota)
	REPLICATION_PAUSED
	REPLICATION_IDLE
	REPLICATION_ACTIVE
	REPLICATION_FETCHED_CHECKPOINT
)

// stateFn represents the state
// as a function that returns the next state.
type stateFn func(*Replication) stateFn

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
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		// valid response, continue with empty remote checkpoint
		logg.LogTo("SYNCTUBE", "404 trying to get checkpoint, continue..")
		event := NewReplicationEvent(FETCH_CHECKPOINT_SUCCEEDED)
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
		r.EventChan <- *event
		// TODO: extract checkpoint
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

func (r Replication) fetchChangesFeed() {

	// TODO: copy fetchRemoteCheckpoint and customize for change feed
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
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {

	}

}

func stateFnPreStarted(r *Replication) stateFn {

	event := <-r.EventChan
	logg.LogTo("SYNCTUBE", "stateFnPreStarted got event: %v", event)
	switch event.Signal {
	case REPLICATION_START:
		logg.LogTo("SYNCTUBE", "stateFnPreStarted got START event: %v", event)

		notification := NewReplicationNotification(REPLICATION_ACTIVE)
		r.NotificationChan <- *notification
		logg.LogTo("SYNCTUBE", "sent notificication: %v", notification)

		go r.fetchTargetCheckpoint()

		logg.LogTo("SYNCTUBE", "Transition from stateFnActiveFetchCheckpoint -> stateFnActive")
		return stateFnActiveFetchCheckpoint

	default:
		logg.LogTo("SYNCTUBE", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnPreStarted

}

func stateFnActiveFetchCheckpoint(r *Replication) stateFn {

	event := <-r.EventChan
	switch event.Signal {
	case REPLICATION_STOP:
		notification := NewReplicationNotification(REPLICATION_STOPPED)
		r.NotificationChan <- *notification
		return nil
	case FETCH_CHECKPOINT_FAILED:
		// TODO: add details to the notification with LastError
		notification := NewReplicationNotification(REPLICATION_STOPPED)
		r.NotificationChan <- *notification
		return nil
	case FETCH_CHECKPOINT_SUCCEEDED:
		logg.LogTo("SYNCTUBE", "Transition from stateFnActiveFetchCheckpoint -> stateFnActive")
		notification := NewReplicationNotification(REPLICATION_FETCHED_CHECKPOINT)

		r.NotificationChan <- *notification

		// TODO: go r.fetchChangesFeed()

		return stateFnActive
	default:
		logg.LogTo("SYNCTUBE", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActiveFetchCheckpoint
}

func stateFnActive(r *Replication) stateFn {

	event := <-r.EventChan
	switch event.Signal {
	case REPLICATION_STOP:
		notification := NewReplicationNotification(REPLICATION_STOPPED)
		r.NotificationChan <- *notification
		return nil
	default:
		logg.LogTo("SYNCTUBE", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActive
}
