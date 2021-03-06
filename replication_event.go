package sgreplicate

type ReplicationEventSignal int

const (
	REPLICATION_UNKNOWN_SIGNAL = ReplicationEventSignal(iota)
	REPLICATION_START
	REPLICATION_STOP
	REPLICATION_PAUSE
	FETCH_CHECKPOINT_SUCCEEDED
	FETCH_CHECKPOINT_FAILED
	FETCH_CHANGES_FEED_FAILED
	FETCH_CHANGES_FEED_SUCCEEDED
	FETCH_REVS_DIFF_FAILED
	FETCH_REVS_DIFF_SUCCEEDED
	FETCH_BULK_GET_FAILED
	FETCH_BULK_GET_SUCCEEDED
	PUSH_BULK_DOCS_FAILED
	PUSH_BULK_DOCS_SUCCEEDED
	PUSH_ATTACHMENT_DOCS_FAILED
	PUSH_ATTACHMENT_DOCS_SUCCEEDED
	PUSH_CHECKPOINT_FAILED
	PUSH_CHECKPOINT_SUCCEEDED
)

type ReplicationEvent struct {
	Signal ReplicationEventSignal
	Data   interface{}
}

func NewReplicationEvent(signal ReplicationEventSignal) *ReplicationEvent {
	return &ReplicationEvent{
		Signal: signal,
	}
}

func (signal ReplicationEventSignal) String() string {
	switch signal {
	case REPLICATION_UNKNOWN_SIGNAL:
		return "REPLICATION_UNKNOWN_SIGNAL"
	case REPLICATION_START:
		return "REPLICATION_START"
	case REPLICATION_STOP:
		return "REPLICATION_STOP"
	case REPLICATION_PAUSE:
		return "REPLICATION_PAUSE"
	case FETCH_CHECKPOINT_SUCCEEDED:
		return "FETCH_CHECKPOINT_SUCCEEDED"
	case FETCH_CHECKPOINT_FAILED:
		return "FETCH_CHECKPOINT_FAILED"
	case FETCH_CHANGES_FEED_SUCCEEDED:
		return "FETCH_CHANGES_FEED_SUCCEEDED"
	case FETCH_CHANGES_FEED_FAILED:
		return "FETCH_CHANGES_FEED_FAILED"
	case FETCH_REVS_DIFF_SUCCEEDED:
		return "FETCH_REVS_DIFF_SUCCEEDED"
	case FETCH_REVS_DIFF_FAILED:
		return "FETCH_REVS_DIFF_FAILED"
	case FETCH_BULK_GET_SUCCEEDED:
		return "FETCH_BULK_GET_SUCCEEDED"
	case FETCH_BULK_GET_FAILED:
		return "FETCH_BULK_GET_FAILED"
	case PUSH_BULK_DOCS_SUCCEEDED:
		return "PUSH_BULK_DOCS_SUCCEEDED"
	case PUSH_BULK_DOCS_FAILED:
		return "PUSH_BULK_DOCS_FAILED"
	case PUSH_ATTACHMENT_DOCS_FAILED:
		return "PUSH_ATTACHMENT_DOCS_FAILED"
	case PUSH_ATTACHMENT_DOCS_SUCCEEDED:
		return "PUSH_ATTACHMENT_DOCS_SUCCEEDED"
	case PUSH_CHECKPOINT_SUCCEEDED:
		return "PUSH_CHECKPOINT_SUCCEEDED"
	case PUSH_CHECKPOINT_FAILED:
		return "PUSH_CHECKPOINT_FAILED"
	}
	return "ERROR_RESOLVING_REPLICATION_EVENT_SIGNAL"

}
