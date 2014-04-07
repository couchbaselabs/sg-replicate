package synctube

type ReplicationStatus int

const (
	REPLICATION_UNKNOWN_STATUS = ReplicationStatus(iota)

	// the replication finished "naturally", eg, successfully
	REPLICATION_STOPPED

	// the replication aborted due to some error or unexpected event
	REPLICATION_ABORTED

	// the replication stopped due to being cancelled by client
	REPLICATION_CANCELLED

	// after replication.Start() is called, this will be emitted
	REPLICATION_ACTIVE

	REPLICATION_FETCHED_CHECKPOINT
	REPLICATION_FETCHED_CHANGES_FEED
	REPLICATION_FETCHED_REVS_DIFF
	REPLICATION_FETCHED_BULK_GET
	REPLICATION_PUSHED_BULK_DOCS
	REPLICATION_PUSHED_CHECKPOINT
)

type ReplicationNotification struct {
	Status ReplicationStatus
	Error  *ReplicationError
	Data   interface{}
}

func NewReplicationNotification(status ReplicationStatus) *ReplicationNotification {
	return &ReplicationNotification{
		Status: status,
	}
}

func (status ReplicationStatus) String() string {
	switch status {
	case REPLICATION_UNKNOWN_STATUS:
		return "REPLICATION_UNKNOWN_STATUS"
	case REPLICATION_STOPPED:
		return "REPLICATION_STOPPED"
	case REPLICATION_CANCELLED:
		return "REPLICATION_CANCELLED"
	case REPLICATION_ABORTED:
		return "REPLICATION_ABORTED"
	case REPLICATION_ACTIVE:
		return "REPLICATION_ACTIVE"
	case REPLICATION_FETCHED_CHECKPOINT:
		return "REPLICATION_FETCHED_CHECKPOINT"
	case REPLICATION_FETCHED_CHANGES_FEED:
		return "REPLICATION_FETCHED_CHANGES_FEED"
	case REPLICATION_FETCHED_REVS_DIFF:
		return "REPLICATION_FETCHED_REVS_DIFF"
	case REPLICATION_FETCHED_BULK_GET:
		return "REPLICATION_FETCHED_BULK_GET"
	case REPLICATION_PUSHED_BULK_DOCS:
		return "REPLICATION_PUSHED_BULK_DOCS"
	case REPLICATION_PUSHED_CHECKPOINT:
		return "REPLICATION_PUSHED_CHECKPOINT"

	}
	return "ERROR_RESOLVING_REPLICATION_STATUS"

}
