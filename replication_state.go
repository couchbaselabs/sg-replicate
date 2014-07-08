package synctube

import (
	"time"

	"github.com/couchbaselabs/logg"
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
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		logg.LogTo("SYNCTUBE", "stateFnActiveFetchCheckpoint: %v", notification)
		r.NotificationChan <- *notification
		logg.LogTo("SYNCTUBE", "going to return nil state")
		return nil
	case FETCH_CHECKPOINT_FAILED:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_ABORTED)
		notification.Error = NewReplicationError(FETCH_CHECKPOINT_FAILED)
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
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		r.NotificationChan <- *notification
		return nil
	case FETCH_CHANGES_FEED_FAILED:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_ABORTED)
		notification.Error = NewReplicationError(FETCH_CHECKPOINT_FAILED)
		r.NotificationChan <- *notification
		return nil
	case FETCH_CHANGES_FEED_SUCCEEDED:

		r.Changes = event.Data.(Changes)

		notification := NewReplicationNotification(REPLICATION_FETCHED_CHANGES_FEED)
		r.NotificationChan <- *notification

		if len(r.Changes.Results) == 0 {
			// nothing to do, so stop
			r.shutdownEventChannel()
			notification := NewReplicationNotification(REPLICATION_STOPPED)
			notification.Data = r.Changes.LastSequence
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
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		logg.LogTo("SYNCTUBE", "stateFnActiveFetchRevDiffs: %v", notification)
		r.NotificationChan <- *notification
		return nil
	case FETCH_REVS_DIFF_FAILED:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_ABORTED)
		notification.Error = NewReplicationError(FETCH_REVS_DIFF_FAILED)
		r.NotificationChan <- *notification
		return nil
	case FETCH_REVS_DIFF_SUCCEEDED:

		r.RevsDiff = event.Data.(RevsDiffResponseMap)

		notification := NewReplicationNotification(REPLICATION_FETCHED_REVS_DIFF)
		r.NotificationChan <- *notification

		if len(r.RevsDiff) == 0 {

			go r.pushCheckpoint()

			logg.LogTo("SYNCTUBE", "Transition from stateFnActiveFetchRevDiffs -> stateFnActivePushCheckpoint")

			return stateFnActivePushCheckpoint

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
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		logg.LogTo("SYNCTUBE", "stateFnActiveFetchBulkGet: %v", notification)
		r.NotificationChan <- *notification
		return nil
	case FETCH_BULK_GET_FAILED:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_ABORTED)
		notification.Error = NewReplicationError(FETCH_BULK_GET_FAILED)
		r.NotificationChan <- *notification
		return nil
	case FETCH_BULK_GET_SUCCEEDED:
		switch event.Data.(type) {
		case []Document:
			r.Documents = event.Data.([]Document)
		default:
			r.shutdownEventChannel()
			logg.LogTo("SYNCTUBE", "Got unexpected type: %v", event.Data)
			notification := NewReplicationNotification(REPLICATION_ABORTED)
			notification.Error = NewReplicationError(FETCH_BULK_GET_FAILED)
			r.NotificationChan <- *notification
			return nil
		}

		notification := NewReplicationNotification(REPLICATION_FETCHED_BULK_GET)
		r.NotificationChan <- *notification

		if len(r.Documents) == 0 {
			r.shutdownEventChannel()
			logg.LogTo("SYNCTUBE", "len(r.DocumentBodies) == 0")
			notification := NewReplicationNotification(REPLICATION_ABORTED)
			notification.Error = NewReplicationError(FETCH_BULK_GET_FAILED)
			r.NotificationChan <- *notification
			return nil
		} else {

			logg.LogTo("SYNCTUBE", "num docs w/o attachemnts: %v", numDocsWithoutAttachments(r.Documents))
			logg.LogTo("SYNCTUBE", "num docs w/ attachemnts: %v", numDocsWithAttachments(r.Documents))
			switch numDocsWithoutAttachments(r.Documents) > 0 {
			case true:
				go r.pushBulkDocs()
				logg.LogTo("SYNCTUBE", "Transition from stateFnActiveFetchBulkGet -> stateFnActivePushBulkDocs")
				return stateFnActivePushBulkDocs
			case false:
				go r.pushAttachmentDocs()
				logg.LogTo("SYNCTUBE", "Transition from stateFnActiveFetchBulkGet -> stateFnActivePushAttachmentDocs")
				return stateFnActivePushAttachmentDocs
			}

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
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		logg.LogTo("SYNCTUBE", "stateFnActivePushBulkDocs: %v", notification)
		r.NotificationChan <- *notification
		return nil
	case PUSH_BULK_DOCS_FAILED:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_ABORTED)
		notification.Error = NewReplicationError(PUSH_BULK_DOCS_FAILED)
		r.NotificationChan <- *notification
		return nil
	case PUSH_BULK_DOCS_SUCCEEDED:

		r.PushedBulkDocs = event.Data.([]DocumentRevisionPair)

		notification := NewReplicationNotification(REPLICATION_PUSHED_BULK_DOCS)
		r.NotificationChan <- *notification

		if len(r.PushedBulkDocs) == 0 {
			r.shutdownEventChannel()
			logg.LogTo("SYNCTUBE", "len(r.PushedBulkDocs) == 0")
			notification := NewReplicationNotification(REPLICATION_ABORTED)
			notification.Error = NewReplicationError(PUSH_BULK_DOCS_FAILED)
			r.NotificationChan <- *notification
			return nil
		} else {

			switch numDocsWithAttachments(r.Documents) > 0 {
			case true:
				go r.pushAttachmentDocs()
				logg.LogTo("SYNCTUBE", "Transition from stateFnActivePushBulkDocs -> stateFnActivePushAttachmentDocs")
				return stateFnActivePushAttachmentDocs

			case false:
				go r.pushCheckpoint()

				logg.LogTo("SYNCTUBE", "Transition from stateFnActivePushBulkDocs -> stateFnActivePushCheckpoint")

				return stateFnActivePushCheckpoint

			}

		}

	default:
		logg.LogTo("SYNCTUBE", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActivePushBulkDocs
}

func stateFnActivePushAttachmentDocs(r *Replication) stateFn {
	logg.LogTo("SYNCTUBE", "stateFnActivePushAttachmentDocs")
	event := <-r.EventChan
	logg.LogTo("SYNCTUBE", "stateFnActivePushAttachmentDocs got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		logg.LogTo("SYNCTUBE", "stateFnActivePushAttachmentDocs: %v", notification)
		r.NotificationChan <- *notification
		return nil
	case PUSH_ATTACHMENT_DOCS_FAILED:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_ABORTED)
		notification.Error = NewReplicationError(PUSH_ATTACHMENT_DOCS_FAILED)
		r.NotificationChan <- *notification
		return nil
	case PUSH_ATTACHMENT_DOCS_SUCCEEDED:

		// TODO: we could record all the docs pushed in the r object

		notification := NewReplicationNotification(REPLICATION_PUSHED_ATTACHMENT_DOCS)
		r.NotificationChan <- *notification

		// TODO: we could also make sure that we pushed the expected number of docs and
		// abort if not.

		go r.pushCheckpoint()

		logg.LogTo("SYNCTUBE", "Transition from stateFnActivePushAttachmentDocs -> stateFnActivePushCheckpoint")

		return stateFnActivePushCheckpoint

	default:
		logg.LogTo("SYNCTUBE", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActivePushAttachmentDocs
}

func stateFnActivePushCheckpoint(r *Replication) stateFn {
	logg.LogTo("SYNCTUBE", "stateFnActivePushCheckpoint")
	event := <-r.EventChan
	logg.LogTo("SYNCTUBE", "stateFnActivePushCheckpoint got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		logg.LogTo("SYNCTUBE", "stateFnActivePushCheckpoint: %v", notification)
		r.NotificationChan <- *notification
		return nil
	case PUSH_CHECKPOINT_FAILED:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_ABORTED)
		notification.Error = NewReplicationError(PUSH_CHECKPOINT_FAILED)
		r.NotificationChan <- *notification
		return nil
	case PUSH_CHECKPOINT_SUCCEEDED:

		notification := NewReplicationNotification(REPLICATION_PUSHED_CHECKPOINT)
		notification.Data = r.Changes.LastSequence
		r.NotificationChan <- *notification

		go r.fetchTargetCheckpoint()

		logg.LogTo("SYNCTUBE", "Transition from stateFnActivePushCheckpoint -> stateFnActiveFetchCheckpoint")
		return stateFnActiveFetchCheckpoint

	default:
		logg.LogTo("SYNCTUBE", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActivePushCheckpoint

}
