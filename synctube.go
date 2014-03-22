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

	for state := stateFnPreStarted; state != nil; {
		state = state(r)
	}
	close(r.EventChan)        // No more events
	close(r.NotificationChan) // No more notifications

}

func (r *Replication) Start() {

	// send a start event to the event channel
	startEvent := NewReplicationEvent(REPLICATION_START)
	r.EventChan <- *startEvent

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
	logg.LogTo("SYNCTUBE", "stateFnActive")
	time.Sleep(time.Second)
	return stateFnActive
}
