package sgreplicate

import "time"

// stateFn represents the state as a function that returns the next state.
type stateFn func(*Replication) stateFn

func stateFnPreStarted(r *Replication) stateFn {

	event := <-r.EventChan
	r.LogTo("Replicate", "stateFnPreStarted got event: %v", event)
	switch event.Signal {
	case REPLICATION_START:
		r.LogTo("Replicate", "stateFnPreStarted got START event: %v", event)

		notification := NewReplicationNotification(REPLICATION_ACTIVE)
		r.NotificationChan <- *notification
		r.LogTo("Replicate", "sent notificication: %v", notification)

		go r.fetchTargetCheckpoint()

		r.LogTo("Replicate", "Transition from stateFnActiveFetchCheckpoint -> stateFnActive")
		return stateFnActiveFetchCheckpoint

	default:
		r.LogTo("Replicate", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnPreStarted

}

func stateFnActiveFetchCheckpoint(r *Replication) stateFn {

	event := <-r.EventChan
	r.LogTo("Replicate", "stateFnActiveFetchCheckpoint got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		r.LogTo("Replicate", "stateFnActiveFetchCheckpoint: %v", notification)
		r.NotificationChan <- *notification
		r.LogTo("Replicate", "going to return nil state")
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

		r.LogTo("Replicate", "call fetchChangesFeed()")
		go r.fetchChangesFeed()

		r.LogTo("Replicate", "Transition from stateFnActiveFetchCheckpoint -> stateFnActiveFetchChangesFeed")

		return stateFnActiveFetchChangesFeed

	default:
		r.LogTo("Replicate", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActiveFetchCheckpoint
}

func stateFnActiveFetchChangesFeed(r *Replication) stateFn {

	r.LogTo("Replicate", "stateFnActiveFetchChangesFeed")
	event := <-r.EventChan
	r.LogTo("Replicate", "stateFnActiveFetchChangesFeed got event: %v", event)
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
			notification.Data = r.Stats
			r.NotificationChan <- *notification
			return nil
		} else {
			r.Stats.SetEndLastSeq(r.Changes.LastSequence)
			go r.fetchRevsDiff()

			r.LogTo("Replicate", "Transition from stateFnActiveFetchChangesFeed -> stateFnActiveFetchRevDiffs")

			return stateFnActiveFetchRevDiffs
		}

	default:
		r.LogTo("Replicate", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActiveFetchChangesFeed
}

func stateFnActiveFetchRevDiffs(r *Replication) stateFn {

	r.LogTo("Replicate", "stateFnActiveFetchRevDiffs")
	event := <-r.EventChan
	r.LogTo("Replicate", "stateFnActiveFetchRevDiffs got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		r.LogTo("Replicate", "stateFnActiveFetchRevDiffs: %v", notification)
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

			r.LogTo("Replicate", "Transition from stateFnActiveFetchRevDiffs -> stateFnActivePushCheckpoint")

			return stateFnActivePushCheckpoint

		} else {

			go r.fetchBulkGet()

			r.LogTo("Replicate", "Transition from stateFnActiveFetchRevDiffs -> stateFnActiveFetchBulkGet")

			return stateFnActiveFetchBulkGet
		}

	default:
		r.LogTo("Replicate", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActiveFetchRevDiffs
}

func stateFnActiveFetchBulkGet(r *Replication) stateFn {
	r.LogTo("Replicate", "stateFnActiveFetchBulkGet")
	event := <-r.EventChan
	r.LogTo("Replicate", "stateFnActiveFetchBulkGet got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		r.LogTo("Replicate", "stateFnActiveFetchBulkGet: %v", notification)
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
			r.LogTo("Replicate", "Got unexpected type: %v", event.Data)
			notification := NewReplicationNotification(REPLICATION_ABORTED)
			notification.Error = NewReplicationError(FETCH_BULK_GET_FAILED)
			r.NotificationChan <- *notification
			return nil
		}

		notification := NewReplicationNotification(REPLICATION_FETCHED_BULK_GET)
		r.NotificationChan <- *notification

		if len(r.Documents) == 0 {
			r.shutdownEventChannel()
			r.LogTo("Replicate", "len(r.DocumentBodies) == 0")
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
				r.LogTo("Replicate", "Transition from stateFnActiveFetchBulkGet -> stateFnActivePushCheckpoint since all docs were _removed:true")
				return stateFnActivePushCheckpoint
			}

			r.LogTo("Replicate", "num docs w/o attachemnts: %v", numDocsWithoutAttachments(r.Documents))
			r.LogTo("Replicate", "num docs w/ attachemnts: %v", numDocsWithAttachments(r.Documents))
			switch numDocsWithoutAttachments(r.Documents) > 0 {
			case true:
				go r.pushBulkDocs()
				r.LogTo("Replicate", "Transition from stateFnActiveFetchBulkGet -> stateFnActivePushBulkDocs")
				return stateFnActivePushBulkDocs
			case false:
				go r.pushAttachmentDocs()
				r.LogTo("Replicate", "Transition from stateFnActiveFetchBulkGet -> stateFnActivePushAttachmentDocs")
				return stateFnActivePushAttachmentDocs
			}

		}

	default:
		r.LogTo("Replicate", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActiveFetchBulkGet

}

func stateFnActivePushBulkDocs(r *Replication) stateFn {
	r.LogTo("Replicate", "stateFnActivePushBulkDocs")
	event := <-r.EventChan
	r.LogTo("Replicate", "stateFnActivePushBulkDocs got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		r.LogTo("Replicate", "stateFnActivePushBulkDocs: %v", notification)
		r.NotificationChan <- *notification
		return nil
	case PUSH_BULK_DOCS_FAILED:
		r.LogTo("Replicate", "stateFnActivePushBulkDocs got PUSH_BULK_DOCS_FAILED")
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_ABORTED)
		notification.Error = NewReplicationError(PUSH_BULK_DOCS_FAILED)
		r.NotificationChan <- *notification
		return nil
	case PUSH_BULK_DOCS_SUCCEEDED:

		r.PushedBulkDocs = event.Data.([]DocumentRevisionPair)

		// if any of the bulk docs have errors, then go back to the FetchCheckpoint state and try again
		if bulkDocsHaveErrors(r.PushedBulkDocs) {
			r.LogTo("Replicate", "BulkDoc response contains partial errors: %+v, going to retry", r.PushedBulkDocs)

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
			r.LogTo("Replicate", "len(r.PushedBulkDocs) == 0")
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
			r.LogTo("Replicate", "Transition from stateFnActivePushBulkDocs -> stateFnActivePushAttachmentDocs")
			return stateFnActivePushAttachmentDocs

		case false:
			go r.pushCheckpoint()

			r.LogTo("Replicate", "Transition from stateFnActivePushBulkDocs -> stateFnActivePushCheckpoint")

			return stateFnActivePushCheckpoint

		}

	default:
		r.LogTo("Replicate", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActivePushBulkDocs
}

func stateFnActivePushAttachmentDocs(r *Replication) stateFn {
	r.LogTo("Replicate", "stateFnActivePushAttachmentDocs")
	event := <-r.EventChan
	r.LogTo("Replicate", "stateFnActivePushAttachmentDocs got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		r.LogTo("Replicate", "stateFnActivePushAttachmentDocs: %v", notification)
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

		r.LogTo("Replicate", "Transition from stateFnActivePushAttachmentDocs -> stateFnActivePushCheckpoint")

		return stateFnActivePushCheckpoint

	default:
		r.LogTo("Replicate", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActivePushAttachmentDocs
}

func stateFnActivePushCheckpoint(r *Replication) stateFn {
	r.LogTo("Replicate", "stateFnActivePushCheckpoint")
	event := <-r.EventChan
	r.LogTo("Replicate", "stateFnActivePushCheckpoint got event: %v", event)
	switch event.Signal {
	case REPLICATION_STOP:
		r.shutdownEventChannel()
		notification := NewReplicationNotification(REPLICATION_CANCELLED)
		r.LogTo("Replicate", "stateFnActivePushCheckpoint: %v", notification)
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

		r.LogTo("Replicate", "Transition from stateFnActivePushCheckpoint -> stateFnActiveFetchCheckpoint")
		return stateFnActiveFetchCheckpoint

	default:
		r.LogTo("Replicate", "Unexpected event: %v", event)
	}

	time.Sleep(time.Second)
	return stateFnActivePushCheckpoint

}
