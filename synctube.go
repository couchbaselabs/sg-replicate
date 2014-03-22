package synctube

import (
	"github.com/couchbaselabs/logg"
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

func (event ReplicationEvent) Status() ReplicationStatus {
	return REPLICATION_STOPPED
}

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

func stateFnPreStarted(r *Replication) stateFn {

	event := <-r.EventChan
	logg.LogTo("SYNCTUBE", "stateFnPreStarted got event: %v", event)
	switch event.Signal {
	case REPLICATION_START:
		logg.LogTo("SYNCTUBE", "stateFnPreStarted got START event: %v", event)

		notification := NewReplicationNotification(REPLICATION_ACTIVE)
		r.NotificationChan <- *notification
		logg.LogTo("SYNCTUBE", "sent notificication: %v", notification)
	}

	return stateFnActive

}

func stateFnActive(r *Replication) stateFn {

	event := <-r.EventChan
	logg.LogTo("SYNCTUBE", "stateFnActive got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		notification := NewReplicationNotification(REPLICATION_STOPPED)
		r.NotificationChan <- *notification
		return nil
	}

	logg.LogTo("SYNCTUBE", "stateFnActive")
	time.Sleep(time.Second)
	return stateFnActive
}
