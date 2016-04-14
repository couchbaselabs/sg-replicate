package sgreplicate

import (
	"time"
	"sync/atomic"
	"github.com/couchbase/clog"
)

// stateFn represents the state as a function that returns the next state.
type stateFn func(*Replication) stateFn

func stateFnPreStarted(r *Replication) stateFn {

	event := <-r.EventChan
	clog.To("Replicate", "stateFnPreStarted got event: %v", event)
	switch event.Signal {
	case REPLICATION_START:
		clog.To("Replicate", "stateFnPreStarted got START event: %v", event)

		notification := NewReplicationNotification(REPLICATION_ACTIVE)
		r.NotificationChan <- *notification
		clog.To("Replicate", "sent notificication: %v", notification)

		go r.fetchTargetCheckpoint()

		clog.To("Replicate", "Transition from stateFnActiveFetchCheckpoint -> stateFnActive")
		return stateFnActiveFetchCheckpoint

	default:
		clog.To("Replicate", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnPreStarted

}

func stateFnActiveFetchCheckpoint(r *Replication) stateFn {

	event := <-r.EventChan
	clog.To("Replicate", "stateFnActiveFetchCheckpoint got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		clog.To("Replicate", "stateFnActiveFetchCheckpoint: %v", notification)
		r.NotificationChan <- *notification
		clog.To("Replicate", "going to return nil state")
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

		clog.To("Replicate", "call fetchChangesFeed()")
		go r.fetchChangesFeed()

		clog.To("Replicate", "Transition from stateFnActiveFetchCheckpoint -> stateFnActiveFetchChangesFeed")

		return stateFnActiveFetchChangesFeed

	default:
		clog.To("Replicate", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActiveFetchCheckpoint
}

func stateFnActiveFetchChangesFeed(r *Replication) stateFn {

	clog.To("Replicate", "stateFnActiveFetchChangesFeed")
	event := <-r.EventChan
	clog.To("Replicate", "stateFnActiveFetchChangesFeed got event: %v", event)
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
			r.Stats.EndLastSeq = r.Changes.LastSequence
			notification.Data = r.Stats
			r.NotificationChan <- *notification
			return nil
		} else {
			r.Stats.EndLastSeq = r.Changes.LastSequence
			go r.fetchRevsDiff()

			clog.To("Replicate", "Transition from stateFnActiveFetchChangesFeed -> stateFnActiveFetchRevDiffs")

			return stateFnActiveFetchRevDiffs
		}

	default:
		clog.To("Replicate", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActiveFetchChangesFeed
}

func stateFnActiveFetchRevDiffs(r *Replication) stateFn {

	clog.To("Replicate", "stateFnActiveFetchRevDiffs")
	event := <-r.EventChan
	clog.To("Replicate", "stateFnActiveFetchRevDiffs got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		clog.To("Replicate", "stateFnActiveFetchRevDiffs: %v", notification)
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

			clog.To("Replicate", "Transition from stateFnActiveFetchRevDiffs -> stateFnActivePushCheckpoint")

			return stateFnActivePushCheckpoint

		} else {

			go r.fetchBulkGet()

			clog.To("Replicate", "Transition from stateFnActiveFetchRevDiffs -> stateFnActiveFetchBulkGet")

			return stateFnActiveFetchBulkGet
		}

	default:
		clog.To("Replicate", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActiveFetchRevDiffs
}

func stateFnActiveFetchBulkGet(r *Replication) stateFn {
	clog.To("Replicate", "stateFnActiveFetchBulkGet")
	event := <-r.EventChan
	clog.To("Replicate", "stateFnActiveFetchBulkGet got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		clog.To("Replicate", "stateFnActiveFetchBulkGet: %v", notification)
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
			atomic.AddUint32(&r.Stats.DocsRead, uint32(len(r.Documents)))
		default:
			r.shutdownEventChannel()
			clog.To("Replicate", "Got unexpected type: %v", event.Data)
			notification := NewReplicationNotification(REPLICATION_ABORTED)
			notification.Error = NewReplicationError(FETCH_BULK_GET_FAILED)
			r.NotificationChan <- *notification
			return nil
		}

		notification := NewReplicationNotification(REPLICATION_FETCHED_BULK_GET)
		r.NotificationChan <- *notification

		if len(r.Documents) == 0 {
			r.shutdownEventChannel()
			clog.To("Replicate", "len(r.DocumentBodies) == 0")
			notification := NewReplicationNotification(REPLICATION_ABORTED)
			notification.Error = NewReplicationError(FETCH_BULK_GET_FAILED)
			r.NotificationChan <- *notification
			return nil
		} else {

			clog.To("Replicate", "num docs w/o attachemnts: %v", numDocsWithoutAttachments(r.Documents))
			clog.To("Replicate", "num docs w/ attachemnts: %v", numDocsWithAttachments(r.Documents))
			switch numDocsWithoutAttachments(r.Documents) > 0 {
			case true:
				go r.pushBulkDocs()
				clog.To("Replicate", "Transition from stateFnActiveFetchBulkGet -> stateFnActivePushBulkDocs")
				return stateFnActivePushBulkDocs
			case false:
				go r.pushAttachmentDocs()
				clog.To("Replicate", "Transition from stateFnActiveFetchBulkGet -> stateFnActivePushAttachmentDocs")
				return stateFnActivePushAttachmentDocs
			}

		}

	default:
		clog.To("Replicate", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActiveFetchBulkGet

}

func stateFnActivePushBulkDocs(r *Replication) stateFn {
	clog.To("Replicate", "stateFnActivePushBulkDocs")
	event := <-r.EventChan
	clog.To("Replicate", "stateFnActivePushBulkDocs got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		clog.To("Replicate", "stateFnActivePushBulkDocs: %v", notification)
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
			clog.To("Replicate", "len(r.PushedBulkDocs) == 0")
			notification := NewReplicationNotification(REPLICATION_ABORTED)
			notification.Error = NewReplicationError(PUSH_BULK_DOCS_FAILED)
			r.NotificationChan <- *notification
			return nil
		} else {

			atomic.AddUint32(&r.Stats.DocsWritten, uint32(len(r.Documents)))
			switch numDocsWithAttachments(r.Documents) > 0 {
			case true:
				go r.pushAttachmentDocs()
				clog.To("Replicate", "Transition from stateFnActivePushBulkDocs -> stateFnActivePushAttachmentDocs")
				return stateFnActivePushAttachmentDocs

			case false:
				go r.pushCheckpoint()

				clog.To("Replicate", "Transition from stateFnActivePushBulkDocs -> stateFnActivePushCheckpoint")

				return stateFnActivePushCheckpoint

			}

		}

	default:
		clog.To("Replicate", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActivePushBulkDocs
}

func stateFnActivePushAttachmentDocs(r *Replication) stateFn {
	clog.To("Replicate", "stateFnActivePushAttachmentDocs")
	event := <-r.EventChan
	clog.To("Replicate", "stateFnActivePushAttachmentDocs got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		clog.To("Replicate", "stateFnActivePushAttachmentDocs: %v", notification)
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

		clog.To("Replicate", "Transition from stateFnActivePushAttachmentDocs -> stateFnActivePushCheckpoint")

		return stateFnActivePushCheckpoint

	default:
		clog.To("Replicate", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActivePushAttachmentDocs
}

func stateFnActivePushCheckpoint(r *Replication) stateFn {
	clog.To("Replicate", "stateFnActivePushCheckpoint")
	event := <-r.EventChan
	clog.To("Replicate", "stateFnActivePushCheckpoint got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		clog.To("Replicate", "stateFnActivePushCheckpoint: %v", notification)
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

		clog.To("Replicate", "Transition from stateFnActivePushCheckpoint -> stateFnActiveFetchCheckpoint")
		return stateFnActiveFetchCheckpoint

	default:
		clog.To("Replicate", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActivePushCheckpoint

}
