package synctube

type ReplicationEventSignal int

const (
	REPLICATION_START = ReplicationEventSignal(iota)
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
)

type ReplicationEvent struct {
	Signal ReplicationEventSignal
	Data   interface{}
	// could have other stuff associated w/ event
}

func NewReplicationEvent(signal ReplicationEventSignal) *ReplicationEvent {
	return &ReplicationEvent{
		Signal: signal,
	}
}
