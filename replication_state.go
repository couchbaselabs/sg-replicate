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
		logg.LogTo("SYNCTUBE", "going to return nil state")
		return nil
	case FETCH_CHECKPOINT_FAILED:
		logg.LogTo("SYNCTUBE", "FETCH_CHECKPOINT_FAILED")
		// TODO: add details to the notification with LastError
		notification := NewReplicationNotification(REPLICATION_STOPPED)
		r.NotificationChan <- *notification
		return nil
	case FETCH_CHECKPOINT_SUCCEEDED:

		checkpoint := event.Data.(Checkpoint)
		r.FetchedTargetCheckpoint = checkpoint

		notification := NewReplicationNotification(REPLICATION_FETCHED_CHECKPOINT)
		r.NotificationChan <- *notification

		logg.LogTo("SYNCTUBE", "call fetchChangesFeed()")
		go r.fetchChangesFeed()

		logg.LogTo("SYNCTUBE", "Transition from stateFnActiveFetchCheckpoint -> stateFnActiveFetchChangesFeed")

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

		r.Changes = event.Data.(Changes)

		notification := NewReplicationNotification(REPLICATION_FETCHED_CHANGES_FEED)
		r.NotificationChan <- *notification

		if len(r.Changes.Results) == 0 {
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
	case FETCH_REVS_DIFF_SUCCEEDED:

		r.RevsDiff = event.Data.(RevsDiffResponseMap)

		notification := NewReplicationNotification(REPLICATION_FETCHED_REVS_DIFF)
		r.NotificationChan <- *notification

		if len(r.RevsDiff) == 0 {
			// nothing to do, so stop
			notification := NewReplicationNotification(REPLICATION_STOPPED)
			r.NotificationChan <- *notification
			return nil
		} else {
			go r.fetchBulkGet()

			logg.LogTo("SYNCTUBE", "Transition from stateFnActiveFetchRevDiffs -> stateFnActiveFetchBulkGet")

			return stateFnActiveFetchBulkGet
		}

	default:
		logg.LogTo("SYNCTUBE", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActiveFetchRevDiffs
}

func stateFnActiveFetchBulkGet(r *Replication) stateFn {
	logg.LogTo("SYNCTUBE", "stateFnActiveFetchBulkGet")
	event := <-r.EventChan
	logg.LogTo("SYNCTUBE", "stateFnActiveFetchBulkGet got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		notification := NewReplicationNotification(REPLICATION_STOPPED)
		r.NotificationChan <- *notification
		return nil
	case FETCH_BULK_GET_FAILED:
		notification := NewReplicationNotification(REPLICATION_STOPPED)
		r.NotificationChan <- *notification
		return nil
	case FETCH_BULK_GET_SUCCEEDED:
		r.DocumentBodies = event.Data.([]DocumentBody)
		notification := NewReplicationNotification(REPLICATION_FETCHED_BULK_GET)
		r.NotificationChan <- *notification

		if len(r.DocumentBodies) == 0 {
			// nothing to do, so stop
			notification := NewReplicationNotification(REPLICATION_STOPPED)
			r.NotificationChan <- *notification
			return nil
		} else {
			go r.pushBulkDocs()

			logg.LogTo("SYNCTUBE", "Transition from stateFnActiveFetchBulkGet -> stateFnActivePushBulkDocs")

			return stateFnActivePushBulkDocs
		}

	default:
		logg.LogTo("SYNCTUBE", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActiveFetchBulkGet

}

func stateFnActivePushBulkDocs(r *Replication) stateFn {
	logg.LogTo("SYNCTUBE", "stateFnActivePushBulkDocs")
	event := <-r.EventChan
	logg.LogTo("SYNCTUBE", "stateFnActivePushBulkDocs got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		notification := NewReplicationNotification(REPLICATION_STOPPED)
		r.NotificationChan <- *notification
		return nil
	case PUSH_BULK_DOCS_FAILED:
		notification := NewReplicationNotification(REPLICATION_STOPPED)
		r.NotificationChan <- *notification
		return nil
	case PUSH_BULK_DOCS_SUCCEEDED:

		r.PushedBulkDocs = event.Data.([]DocumentRevisionPair)

		notification := NewReplicationNotification(REPLICATION_PUSHED_BULK_DOCS)
		r.NotificationChan <- *notification

		if len(r.PushedBulkDocs) == 0 {
			// nothing to do, so stop
			logg.LogTo("SYNCTUBE", "nothing to do, so stop")
			notification := NewReplicationNotification(REPLICATION_STOPPED)
			r.NotificationChan <- *notification
			return nil
		} else {
			go r.pushCheckpoint()

			logg.LogTo("SYNCTUBE", "Transition from stateFnActivePushBulkDocs -> stateFnActivePushCheckpoint")

			return stateFnActivePushCheckpoint
		}

	default:
		logg.LogTo("SYNCTUBE", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActivePushBulkDocs
}

func stateFnActivePushCheckpoint(r *Replication) stateFn {
	logg.LogTo("SYNCTUBE", "stateFnActivePushCheckpoint")
	event := <-r.EventChan
	logg.LogTo("SYNCTUBE", "stateFnActivePushCheckpoint got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		notification := NewReplicationNotification(REPLICATION_STOPPED)
		r.NotificationChan <- *notification
		return nil
	case PUSH_CHECKPOINT_FAILED:

		r.LastError = NewReplicationError("PUSH_CHECKPOINT_FAILED")

		notification := NewReplicationNotification(REPLICATION_STOPPED)
		r.NotificationChan <- *notification
		return nil
	case PUSH_CHECKPOINT_SUCCEEDED:

		r.LastSequencePushed = r.Changes.LastSequence

		notification := NewReplicationNotification(REPLICATION_PUSHED_CHECKPOINT)
		r.NotificationChan <- *notification

		// TOOD: r.resetExtendedState()

		go r.fetchTargetCheckpoint()

		logg.LogTo("SYNCTUBE", "Transition from stateFnActivePushCheckpoint -> stateFnActiveFetchCheckpoint")
		return stateFnActiveFetchCheckpoint

	default:
		logg.LogTo("SYNCTUBE", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActivePushCheckpoint

}
