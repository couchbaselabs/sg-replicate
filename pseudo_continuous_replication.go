package synctube

import (
	"github.com/couchbaselabs/logg"
)

type PseudoContinuousReplicationEvent int

const (
	UNKNOWN_EVENT = PseudoContinuousReplicationEvent(iota)
	STOP
)

type PseudoContinuousReplicationNotification int

const (
	UNKNOWN_NOTIFICATION = PseudoContinuousReplicationNotification(iota)
	CATCHING_UP
	CAUGHT_UP
	CANCELLED
	FAILED
)



// Wraps a Replication (which should be renamed to "OneShotReplication") and
// drives it, creating a "pseudo-continuous replication".  It's "pseudo", because
// the fact that it's a continuous replication is not baked into the state machine.
// A "pure" continuous replication would have it's own state machine implementation.
type PseudoContinuousReplication struct {

	// parameters of the wrapped replication
	ReplicationParameters ReplicationParameters

	// the notifications we send out to clients of this api
	NotificationChan      chan PseudoContinuousReplicationNotification
	
	// internal events
	EventChan             chan PseudoContinuousReplicationEvent

}

func NewPseudoContinuousReplication(params ReplicationParameters, notificationChan chan PseudoContinuousReplicationNotification) *PseudoContinuousReplication {

	eventChan := make(chan PseudoContinuousReplicationEvent)

	replication := &PseudoContinuousReplication{
		ReplicationParameters: params,
		NotificationChan:      notificationChan,
		EventChan:             eventChan,
	}

	// spawn a go-routine that reads from event channel and acts on events
	go replication.processEvents()

	return replication

}

func (r PseudoContinuousReplication) Stop() {
	r.EventChan <- STOP
}

func (r *PseudoContinuousReplication) processEvents() {

	for {

		r.NotificationChan <- CATCHING_UP

		// --- run a replication to catch up
		notificationChan := make(chan ReplicationNotification)
		replication := NewReplication(r.ReplicationParameters, notificationChan)
		replication.Start()

		// wait until we get a stopped or an abort from replication (or user cancels)
		select {
		case event := <- r.EventChan:
			switch event {
			case STOP:
				r.NotificationChan <- CANCELLED
				return 
			default:
				logg.LogTo("SYNCTUBE", "Got unknown event, ignoring: %v", event)
			}
		case notification := <- notificationChan
			switch notification.Status {
			case REPLICATION_STOPPED:
				logg.LogTo("SYNCTUBE", "Replication stopped, caught up")
				r.NotificationChan <- CAUGHT_UP
				// do longpoll on changes feed until something changes, then go top of loop
				
				
			case REPLICATION_ABORTED:
				// TODO: should chillax a while in this case
				logg.LogTo("SYNCTUBE", "Replication aborted .. try again")
			}
		}
		

	}

}










