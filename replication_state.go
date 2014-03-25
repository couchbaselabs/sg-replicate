package synctube

import (
	"github.com/couchbaselabs/logg"
	"time"
)

// stateFn represents the state as a function that returns the next state.
type stateFn func(*Replication) stateFn

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
	logg.LogTo("SYNCTUBE", "stateFnActiveFetchCheckpoint got event: %v", event)
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

		logg.LogTo("SYNCTUBE", "event.data: %v", event.Data)
		dataString := event.Data.(string)
		logg.LogTo("SYNCTUBE", "event.string: %v", dataString)
		checkpoint := Checkpoint{LastSequence: dataString}
		r.FetchedTargetCheckpoint = checkpoint

		notification := NewReplicationNotification(REPLICATION_FETCHED_CHECKPOINT)
		r.NotificationChan <- *notification

		go r.fetchChangesFeed()

		return stateFnActive
	default:
		logg.LogTo("SYNCTUBE", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActiveFetchCheckpoint
}

func stateFnActive(r *Replication) stateFn {

	logg.LogTo("SYNCTUBE", "stateFnActive")
	event := <-r.EventChan
	logg.LogTo("SYNCTUBE", "stateFnActive got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		notification := NewReplicationNotification(REPLICATION_STOPPED)
		r.NotificationChan <- *notification
		return nil
	case FETCH_CHANGES_FEED_FAILED:
		notification := NewReplicationNotification(REPLICATION_FETCH_CHANGES_FEED_FAILED)
		r.NotificationChan <- *notification
		return nil

	default:
		logg.LogTo("SYNCTUBE", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActive
}
