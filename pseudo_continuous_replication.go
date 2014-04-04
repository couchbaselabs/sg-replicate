package synctube

import (
	"github.com/couchbaselabs/logg"
	"time"
)

type ContinuousReplicationEvent int

const (
	UNKNOWN_EVENT = ContinuousReplicationEvent(iota)
	STOP
)

type ContinuousReplicationNotification int

const (
	UNKNOWN_NOTIFICATION = ContinuousReplicationNotification(iota)
	CATCHING_UP
	CAUGHT_UP
	CANCELLED
	ABORTED_WAITING_TO_RETRY
	FAILED
)

// Wraps a Replication (which should be renamed to "OneShotReplication") and
// drives it, creating a "pseudo-continuous replication".  It's "pseudo", because
// the fact that it's a continuous replication is not baked into the state machine.
// A "pure" continuous replication would have it's own state machine implementation.
type ContinuousReplication struct {

	// parameters of the wrapped replication
	ReplicationParameters ReplicationParameters

	// the notifications we send out to clients of this api
	NotificationChan chan ContinuousReplicationNotification

	// internal events
	EventChan chan ContinuousReplicationEvent
}

func NewContinuousReplication(params ReplicationParameters, notificationChan chan ContinuousReplicationNotification) *ContinuousReplication {

	eventChan := make(chan ContinuousReplicationEvent)

	replication := &ContinuousReplication{
		ReplicationParameters: params,
		NotificationChan:      notificationChan,
		EventChan:             eventChan,
	}

	// spawn a go-routine that reads from event channel and acts on events
	go replication.processEvents()

	return replication

}

func (r ContinuousReplication) Stop() {
	r.EventChan <- STOP
}

func (r *ContinuousReplication) processEvents() {

	// nil out the EventChan after the event loop has finished.
	defer func() { r.EventChan = nil }()

	defer close(r.NotificationChan) // No more notifications

	for state := stateFnCatchingUp; state != nil; {
		state = state(r)
		logg.LogTo("SYNCTUBE", "continuous repl new state: %v", state)
	}
	logg.LogTo("SYNCTUBE", "continuous repl processEvents() is done")

}

func (r ContinuousReplication) fetchLongpollChanges(responseChan chan bool) {

	// TODO: remove fake and do real impl
	<-time.After(10 * time.Second)
	responseChan <- true

}

// represents the state as a function that returns the next state.
type stateFnContinuous func(*ContinuousReplication) stateFnContinuous

func stateFnCatchingUp(r *ContinuousReplication) stateFnContinuous {

	r.NotificationChan <- CATCHING_UP

	// run a replication to catch up
	notificationChan := make(chan ReplicationNotification)
	replication := NewReplication(r.ReplicationParameters, notificationChan)
	replication.Start()

	for {
		select {
		case event := <-r.EventChan:
			switch event {
			case STOP:
				r.NotificationChan <- CANCELLED
				return nil
			default:
				logg.LogTo("SYNCTUBE", "Got unknown event, ignoring: %v", event)
			}
		case notification := <-notificationChan:
			switch notification.Status {
			case REPLICATION_STOPPED:
				logg.LogTo("SYNCTUBE", "Replication stopped, caught up")
				return stateFnWaitNewChanges
			case REPLICATION_ABORTED:
				logg.LogTo("SYNCTUBE", "Replication aborted .. try again")
				return stateFnBackoffRetry
			}
		}

	}

	// should never make it this far
	return nil

}

func stateFnWaitNewChanges(r *ContinuousReplication) stateFnContinuous {

	r.NotificationChan <- CAUGHT_UP

	for {

		resultChan := make(chan bool)
		go r.fetchLongpollChanges(resultChan)

		select {
		case event := <-r.EventChan:
			switch event {
			case STOP:
				r.NotificationChan <- CANCELLED
				return nil
			default:
				logg.LogTo("SYNCTUBE", "Got unknown event, ignoring: %v", event)
			}
		case foundNewChanges := <-resultChan:
			switch foundNewChanges {
			case true:
				logg.LogTo("SYNCTUBE", "Got new longpoll changes")
				return stateFnCatchingUp
			case false:
				logg.LogTo("SYNCTUBE", "No new longpoll changes yet")
				// try again ..
			}

		}

	}

	// should never get here
	return nil

}

func stateFnBackoffRetry(r *ContinuousReplication) stateFnContinuous {

	r.NotificationChan <- ABORTED_WAITING_TO_RETRY

	for {
		select {
		case event := <-r.EventChan:
			switch event {
			case STOP:
				r.NotificationChan <- CANCELLED
				return nil
			default:
				logg.LogTo("SYNCTUBE", "Got unknown event, ignoring: %v", event)
			}
		case <-time.After(15 * time.Second):
			logg.LogTo("SYNCTUBE", "Done waiting to retry")
			return stateFnCatchingUp

		}

	}

	// should never get here
	return nil

}
