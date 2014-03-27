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
		logg.LogTo("SYNCTUBE", "Transition from stateFnActiveFetchCheckpoint -> stateFnActiveFetchChangesFeed")
		dataString := event.Data.(string)
		logg.LogTo("SYNCTUBE", "event.string: %v", dataString)
		checkpoint := Checkpoint{LastSequence: dataString}
		r.FetchedTargetCheckpoint = checkpoint

		notification := NewReplicationNotification(REPLICATION_FETCHED_CHECKPOINT)
		r.NotificationChan <- *notification

		go r.fetchChangesFeed()

		return stateFnActiveFetchChangesFeed
	default:
		logg.LogTo("SYNCTUBE", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActiveFetchCheckpoint
}

func stateFnActiveFetchChangesFeed(r *Replication) stateFn {

	logg.LogTo("SYNCTUBE", "stateFnActiveFetchChangesFeed")
	event := <-r.EventChan
	logg.LogTo("SYNCTUBE", "stateFnActiveFetchChangesFeed got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		notification := NewReplicationNotification(REPLICATION_STOPPED)
		r.NotificationChan <- *notification
		return nil
	case FETCH_CHANGES_FEED_FAILED:
		notification := NewReplicationNotification(REPLICATION_STOPPED)
		r.NotificationChan <- *notification
		return nil
	case FETCH_CHANGES_FEED_SUCCEEDED:

		changes := event.Data.(Changes)
		r.Changes = changes

		notification := NewReplicationNotification(REPLICATION_FETCHED_CHANGES_FEED)
		r.NotificationChan <- *notification

		if len(changes.Results) == 0 {
			// nothing to do, so stop
			notification := NewReplicationNotification(REPLICATION_STOPPED)
			r.NotificationChan <- *notification
			return nil
		} else {
			go r.fetchRevsDiff()

			logg.LogTo("SYNCTUBE", "Transition from stateFnActiveFetchChangesFeed -> stateFnActiveFetchRevDiffs")

			return stateFnActiveFetchRevDiffs
		}

	default:
		logg.LogTo("SYNCTUBE", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActiveFetchChangesFeed
}

func stateFnActiveFetchRevDiffs(r *Replication) stateFn {

	logg.LogTo("SYNCTUBE", "stateFnActiveFetchRevDiffs")
	event := <-r.EventChan
	logg.LogTo("SYNCTUBE", "stateFnActiveFetchRevDiffs got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		notification := NewReplicationNotification(REPLICATION_STOPPED)
		r.NotificationChan <- *notification
		return nil
	case FETCH_REVS_DIFF_FAILED:
		notification := NewReplicationNotification(REPLICATION_STOPPED)
		r.NotificationChan <- *notification
		return nil

	default:
		logg.LogTo("SYNCTUBE", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActiveFetchRevDiffs
}
