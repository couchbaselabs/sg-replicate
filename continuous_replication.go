package synctube

import (
	"encoding/json"
	"github.com/couchbaselabs/logg"
	"io/ioutil"
	"net/http"
	"time"
)

type ContinuousReplicationEvent int

const (
	UNKNOWN_EVENT = ContinuousReplicationEvent(iota)
	STOP
)

func (event ContinuousReplicationEvent) String() string {
	switch event {
	case STOP:
		return "STOP"
	default:
		return "UNKNOWN_EVENT"
	}
}

type ContinuousReplicationNotification int

const (
	UNKNOWN_NOTIFICATION = ContinuousReplicationNotification(iota)
	CATCHING_UP
	CAUGHT_UP
	CANCELLED
	ABORTED_WAITING_TO_RETRY
	FAILED
)

func (notification ContinuousReplicationNotification) String() string {
	switch notification {
	case CATCHING_UP:
		return "CATCHING_UP"
	case CAUGHT_UP:
		return "CAUGHT_UP"
	case CANCELLED:
		return "CANCELLED"
	case ABORTED_WAITING_TO_RETRY:
		return "ABORTED_WAITING_TO_RETRY"
	case FAILED:
		return "FAILED"
	default:
		return "UNKNOWN_NOTIFICATION"
	}
}

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

	// factory to create replications
	Factory ReplicationFactory

	// the amount of time to wait after an aborted replication before retrying
	AbortedReplicationRetryTime time.Duration

	// The last sequenced pushed by last wrapped replication that was run
	LastSequencePushed sequenceNumber
}

type Runnable interface {
	Start() error
}

type ReplicationFactory func(ReplicationParameters, chan ReplicationNotification) Runnable

func NewContinuousReplication(params ReplicationParameters, factory ReplicationFactory, notificationChan chan ContinuousReplicationNotification, retryTime time.Duration) *ContinuousReplication {

	eventChan := make(chan ContinuousReplicationEvent)

	replication := &ContinuousReplication{
		ReplicationParameters:       params,
		NotificationChan:            notificationChan,
		EventChan:                   eventChan,
		Factory:                     factory,
		AbortedReplicationRetryTime: retryTime,
	}

	// spawn a go-routine that reads from event channel and acts on events
	go replication.processEvents()

	return replication

}

func (r ContinuousReplication) Stop() {
	r.EventChan <- STOP
}

func (r *ContinuousReplication) processEvents() {

	logg.LogTo("SYNCTUBE", "ContinuousReplication.processEvents()")

	// nil out the EventChan after the event loop has finished.
	defer func() { r.EventChan = nil }()

	defer close(r.NotificationChan) // No more notifications

	for state := stateFnCatchingUp; state != nil; {
		logg.LogTo("SYNCTUBE", "continuous repl state: %v", state)
		state = state(r)
		logg.LogTo("SYNCTUBE", "continuous repl new state: %v", state)
	}
	logg.LogTo("SYNCTUBE", "continuous repl processEvents() is done")

}

func (r ContinuousReplication) fetchLongpollChanges(responseChan chan bool) {

	changesFeedParams := NewChangesFeedParams()
	changesFeedParams.feedType = FEED_TYPE_LONGPOLL
	changesFeedParams.since = r.LastSequencePushed
	changesFeedUrl := r.ReplicationParameters.getSourceChangesFeedUrl(*changesFeedParams)

	logg.LogTo("SYNCTUBE", "fetching longpoll changes at url: %v", changesFeedUrl)

	client := &http.Client{}
	req, _ := http.NewRequest("GET", changesFeedUrl, nil)
	resp, err := client.Do(req)
	logg.LogTo("SYNCTUBE", "longpoll changes feed resp: %v, err: %v", resp, err)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error getting longpoll changes feed: %v", err)
		responseChan <- false
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		logg.LogTo("SYNCTUBE", "Error getting longpoll changes feed.  Resp: %v", resp)
		responseChan <- false
		return
	}

	bodyText, _ := ioutil.ReadAll(resp.Body)
	changes := Changes{}
	err = json.Unmarshal(bodyText, &changes)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error unmarshalling longpoll changes")
		logg.LogError(err)
		responseChan <- false
		return
	}

	gotChanges := len(changes.Results) > 0
	responseChan <- gotChanges

}

func (r ContinuousReplication) startOneShotReplication() chan ReplicationNotification {

	notificationChan := make(chan ReplicationNotification)
	replication := r.Factory(r.ReplicationParameters, notificationChan)
	replication.Start()
	return notificationChan

}

// represents the state as a function that returns the next state.
type stateFnContinuous func(*ContinuousReplication) stateFnContinuous

func stateFnCatchingUp(r *ContinuousReplication) stateFnContinuous {

	r.NotificationChan <- CATCHING_UP

	// run a replication to catch up
	notificationChan := r.startOneShotReplication()

	for {
		logg.LogTo("SYNCTUBE", "stateFnCatchingUp, waiting for event")
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
				r.LastSequencePushed = sequenceNumber(notification.Data.(int))
				return stateFnWaitNewChanges
			case REPLICATION_ABORTED:
				logg.LogTo("SYNCTUBE", "Replication aborted .. try again")
				return stateFnBackoffRetry
			default:
				logg.LogTo("SYNCTUBE", "Unexpected notification, ignore")
			}
		}

	}

}

func stateFnWaitNewChanges(r *ContinuousReplication) stateFnContinuous {

	r.NotificationChan <- CAUGHT_UP

	for {

		resultChan := make(chan bool)
		go r.fetchLongpollChanges(resultChan)

		logg.LogTo("SYNCTUBE", "stateFnWaitNewChanges, waiting for event")
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

}

func stateFnBackoffRetry(r *ContinuousReplication) stateFnContinuous {

	logg.LogTo("SYNCTUBE", "entered stateFnBackoffRertry")

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
		case <-time.After(r.AbortedReplicationRetryTime):
			logg.LogTo("SYNCTUBE", "Done waiting to retry")
			return stateFnCatchingUp

		}

	}

}
