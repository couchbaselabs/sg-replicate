package synctube

import (
	"fmt"
	"github.com/couchbaselabs/logg"
	"github.com/mreiferson/go-httpclient"
	"net/http"
	"net/url"
	"time"
)

type ReplicationParameters struct {
	Source     *url.URL
	Target     *url.URL
	Continuous bool
}

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

type ReplicationEventSignal int

const (
	REPLICATION_START = ReplicationEventSignal(iota)
	REPLICATION_STOP
	REPLICATION_PAUSE
	FETCH_CHECKPOINT_SUCCEEDED
	FETCH_CHECKPOINT_FAILED
)

type ReplicationEvent struct {
	Signal ReplicationEventSignal
	// could have other stuff associated w/ event
}

func NewReplicationEvent(signal ReplicationEventSignal) *ReplicationEvent {
	return &ReplicationEvent{
		Signal: signal,
	}
}

type ReplicationStatus int

const (
	REPLICATION_STOPPED = ReplicationStatus(iota)
	REPLICATION_PAUSED
	REPLICATION_IDLE
	REPLICATION_ACTIVE
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

func (r *Replication) processEvents() {

	defer close(r.EventChan)        // No more events
	defer close(r.NotificationChan) // No more notifications

	for state := stateFnPreStarted; state != nil; {
		state = state(r)
	}

}

func (r *Replication) Start() {
	event := NewReplicationEvent(REPLICATION_START)
	r.EventChan <- *event
}

func (r *Replication) Stop() {
	event := NewReplicationEvent(REPLICATION_STOP)
	r.EventChan <- *event
}

func (r *Replication) baseTargetUrl() string {
	return r.Parameters.Target.String()
}

func (r *Replication) fetchTargetCheckpoint() {

	// fetch the checkpoint document
	// on the target.

	checkpoint := "foo" // TODO
	destUrl := fmt.Sprintf("%s/_local/%s", r.baseTargetUrl(), checkpoint)

	transport := &httpclient.Transport{
		ConnectTimeout:        60 * time.Second,
		RequestTimeout:        60 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
	}
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
