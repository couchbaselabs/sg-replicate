package sgreplicate

import (
	"time"

	"github.com/couchbase/clog"
)

// stateFn represents the state as a function that returns the next state.
type stateFn func(*Replication) stateFn

func stateFnPreStarted(r *Replication) stateFn {

	event := <-r.EventChan
	r.log(clog.LevelDebug, "stateFnPreStarted got event: %v", event)
	switch event.Signal {
	case REPLICATION_START:
		r.log(clog.LevelDebug, "stateFnPreStarted got START event: %v", event)

		notification := NewReplicationNotification(REPLICATION_ACTIVE)
		r.NotificationChan <- *notification
		r.log(clog.LevelDebug, "sent notification: %v", notification)

		go r.fetchTargetCheckpoint()

		r.log(clog.LevelDebug, "Transition from stateFnActiveFetchCheckpoint -> stateFnActive")
		return stateFnActiveFetchCheckpoint

	default:
		r.log(clog.LevelDebug, "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnPreStarted

}

func stateFnActiveFetchCheckpoint(r *Replication) stateFn {

	event := <-r.EventChan
	r.log(clog.LevelDebug, "stateFnActiveFetchCheckpoint got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		r.log(clog.LevelDebug, "stateFnActiveFetchCheckpoint: %v", notification)
		r.NotificationChan <- *notification
		r.log(clog.LevelDebug, "going to return nil state")
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

		r.log(clog.LevelDebug, "call fetchChangesFeed()")
		go r.fetchChangesFeed()

		r.log(clog.LevelDebug, "Transition from stateFnActiveFetchCheckpoint -> stateFnActiveFetchChangesFeed")

		return stateFnActiveFetchChangesFeed

	default:
		r.log(clog.LevelDebug, "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActiveFetchCheckpoint
}

func stateFnActiveFetchChangesFeed(r *Replication) stateFn {

	r.log(clog.LevelDebug, "stateFnActiveFetchChangesFeed")
	event := <-r.EventChan
	r.log(clog.LevelDebug, "stateFnActiveFetchChangesFeed got event: %v", event)
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
			r.Stats.SetEndLastSeq(r.Changes.LastSequence)
			notification.Data = r.Changes.LastSequence
			r.NotificationChan <- *notification
			return nil
		} else {
			r.Stats.SetEndLastSeq(r.Changes.LastSequence)
			go r.fetchRevsDiff()

			r.log(clog.LevelDebug, "Transition from stateFnActiveFetchChangesFeed -> stateFnActiveFetchRevDiffs")

			return stateFnActiveFetchRevDiffs
		}

	default:
		r.log(clog.LevelDebug, "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActiveFetchChangesFeed
}

func stateFnActiveFetchRevDiffs(r *Replication) stateFn {

	r.log(clog.LevelDebug, "stateFnActiveFetchRevDiffs")
	event := <-r.EventChan
	r.log(clog.LevelDebug, "stateFnActiveFetchRevDiffs got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		r.log(clog.LevelDebug, "stateFnActiveFetchRevDiffs: %v", notification)
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

			r.log(clog.LevelDebug, "Transition from stateFnActiveFetchRevDiffs -> stateFnActivePushCheckpoint")

			return stateFnActivePushCheckpoint

		} else {

			go r.fetchBulkGet()

			r.log(clog.LevelDebug, "Transition from stateFnActiveFetchRevDiffs -> stateFnActiveFetchBulkGet")

			return stateFnActiveFetchBulkGet
		}

	default:
		r.log(clog.LevelDebug, "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActiveFetchRevDiffs
}

func stateFnActiveFetchBulkGet(r *Replication) stateFn {
	r.log(clog.LevelDebug, "stateFnActiveFetchBulkGet")
	event := <-r.EventChan
	r.log(clog.LevelDebug, "stateFnActiveFetchBulkGet got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		r.log(clog.LevelDebug, "stateFnActiveFetchBulkGet: %v", notification)
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
			r.Stats.AddDocsRead(uint32(len(r.Documents)))
		default:
			r.shutdownEventChannel()
			r.log(clog.LevelDebug, "Got unexpected type: %v", event.Data)
			notification := NewReplicationNotification(REPLICATION_ABORTED)
			notification.Error = NewReplicationError(FETCH_BULK_GET_FAILED)
			r.NotificationChan <- *notification
			return nil
		}

		notification := NewReplicationNotification(REPLICATION_FETCHED_BULK_GET)
		r.NotificationChan <- *notification

		if len(r.Documents) == 0 {
			r.shutdownEventChannel()
			r.log(clog.LevelDebug, "len(r.DocumentBodies) == 0")
			notification := NewReplicationNotification(REPLICATION_ABORTED)
			notification.Error = NewReplicationError(FETCH_BULK_GET_FAILED)
			r.NotificationChan <- *notification
			return nil
		} else {

			// Filter out the _removed:true docs since there is no point in pushing them via _bulk_docs
			r.Documents = filterRemovedDocs(r.Documents)

			if len(r.Documents) == 0 {
				// If there are no docs left after filtering out _removed:true docs, then
				// skip the _bulk_docs step and go straight to the pushCheckpoint step
				go r.pushCheckpoint()
				r.log(clog.LevelDebug, "Transition from stateFnActiveFetchBulkGet -> stateFnActivePushCheckpoint since all docs were _removed:true")
				return stateFnActivePushCheckpoint
			}

			r.log(clog.LevelDebug, "num docs w/o attachemnts: %v", numDocsWithoutAttachments(r.Documents))
			r.log(clog.LevelDebug, "num docs w/ attachemnts: %v", numDocsWithAttachments(r.Documents))
			switch numDocsWithoutAttachments(r.Documents) > 0 {
			case true:
				go r.pushBulkDocs()
				r.log(clog.LevelDebug, "Transition from stateFnActiveFetchBulkGet -> stateFnActivePushBulkDocs")
				return stateFnActivePushBulkDocs
			case false:
				go r.pushAttachmentDocs()
				r.log(clog.LevelDebug, "Transition from stateFnActiveFetchBulkGet -> stateFnActivePushAttachmentDocs")
				return stateFnActivePushAttachmentDocs
			}

		}

	default:
		r.log(clog.LevelDebug, "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActiveFetchBulkGet

}

func stateFnActivePushBulkDocs(r *Replication) stateFn {
	r.log(clog.LevelDebug, "stateFnActivePushBulkDocs")
	event := <-r.EventChan
	r.log(clog.LevelDebug, "stateFnActivePushBulkDocs got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		r.log(clog.LevelDebug, "stateFnActivePushBulkDocs: %v", notification)
		r.NotificationChan <- *notification
		return nil
	case PUSH_BULK_DOCS_FAILED:
		r.log(clog.LevelDebug, "stateFnActivePushBulkDocs got PUSH_BULK_DOCS_FAILED")
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_ABORTED)
		notification.Error = NewReplicationError(PUSH_BULK_DOCS_FAILED)
		r.NotificationChan <- *notification
		return nil
	case PUSH_BULK_DOCS_SUCCEEDED:

		r.PushedBulkDocs = event.Data.([]DocumentRevisionPair)

		// if any of the bulk docs have recoverable errors, then go back to the FetchCheckpoint state and try again
		if bulkDocsHaveRecoverableErrors(r.PushedBulkDocs) {
			r.log(clog.LevelDebug, "BulkDoc response contains partial errors: %+v, going to retry", r.PushedBulkDocs)

			go func() {
				// sleep for 1 second to avoid a tight loop hammering SG
				time.Sleep(time.Second)

				r.fetchTargetCheckpoint()
			}()

			return stateFnActiveFetchCheckpoint
		}

		// if the response was empty (how can this even happen!?) then abort replication
		if len(r.PushedBulkDocs) == 0 {
			r.shutdownEventChannel()
			r.log(clog.LevelDebug, "len(r.PushedBulkDocs) == 0")
			notification := NewReplicationNotification(REPLICATION_ABORTED)
			notification.Error = NewReplicationError(PUSH_BULK_DOCS_FAILED)
			r.NotificationChan <- *notification
			return nil
		}

		// update stats:
		// TODO: is this accurate when some of the docs had attachments and still need to be pushed?
		r.Stats.AddDocsWritten(uint32(len(r.Documents)))

		// if we made it this far, send a notification that the bulk_docs succeeded
		notification := NewReplicationNotification(REPLICATION_PUSHED_BULK_DOCS)
		r.NotificationChan <- *notification

		// if any of the docs have attachments, we need to push those
		// separately.  otherwise, just push the checkpoint with the latest sequence
		// returned from the previous changes feed request
		switch numDocsWithAttachments(r.Documents) > 0 {
		case true:
			go r.pushAttachmentDocs()
			r.log(clog.LevelDebug, "Transition from stateFnActivePushBulkDocs -> stateFnActivePushAttachmentDocs")
			return stateFnActivePushAttachmentDocs

		case false:
			go r.pushCheckpoint()

			r.log(clog.LevelDebug, "Transition from stateFnActivePushBulkDocs -> stateFnActivePushCheckpoint")

			return stateFnActivePushCheckpoint

		}

	default:
		r.log(clog.LevelDebug, "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActivePushBulkDocs
}

func stateFnActivePushAttachmentDocs(r *Replication) stateFn {
	r.log(clog.LevelDebug, "stateFnActivePushAttachmentDocs")
	event := <-r.EventChan
	r.log(clog.LevelDebug, "stateFnActivePushAttachmentDocs got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		r.log(clog.LevelDebug, "stateFnActivePushAttachmentDocs: %v", notification)
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

		r.log(clog.LevelDebug, "Transition from stateFnActivePushAttachmentDocs -> stateFnActivePushCheckpoint")

		return stateFnActivePushCheckpoint

	default:
		r.log(clog.LevelDebug, "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActivePushAttachmentDocs
}

func stateFnActivePushCheckpoint(r *Replication) stateFn {
	r.log(clog.LevelDebug, "stateFnActivePushCheckpoint")
	event := <-r.EventChan
	r.log(clog.LevelDebug, "stateFnActivePushCheckpoint got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		r.log(clog.LevelDebug, "stateFnActivePushCheckpoint: %v", notification)
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

		r.log(clog.LevelDebug, "Transition from stateFnActivePushCheckpoint -> stateFnActiveFetchCheckpoint")
		return stateFnActiveFetchCheckpoint

	default:
		r.log(clog.LevelDebug, "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActivePushCheckpoint

}
