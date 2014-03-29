package synctube

type ReplicationStatus int

const (
	REPLICATION_STOPPED = ReplicationStatus(iota)
	REPLICATION_PAUSED
	REPLICATION_IDLE
	REPLICATION_ACTIVE
	REPLICATION_FETCHED_CHECKPOINT
	REPLICATION_FETCHED_CHANGES_FEED
	REPLICATION_FETCHED_REVS_DIFF
	REPLICATION_FETCHED_BULK_GET
	REPLICATION_PUSHED_BULK_DOCS
)

type ReplicationNotification struct {
	Status ReplicationStatus
	// could have other stuff associated w/ notification
}

func NewReplicationNotification(status ReplicationStatus) *ReplicationNotification {
	return &ReplicationNotification{
		Status: status,
	}
}
