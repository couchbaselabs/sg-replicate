package synctube

import (
	"net/url"
)

type ReplicationParameters struct {
	Source     *url.URL
	Target     *url.URL
	Continuous bool
}

type ReplicationEvent struct {
}

type Replication struct {
	Parameters ReplicationParameters
}

type ReplicationStatus int

const (
	REPLICATION_STOPPED = ReplicationStatus(iota)
	REPLICATION_OFFLINE
	REPLICATION_IDLE
	REPLICATION_ACTIVE
)

func (event ReplicationEvent) Status() ReplicationStatus {
	return REPLICATION_STOPPED
}

func NewReplication(params ReplicationParameters, eventChan chan ReplicationEvent) *Replication {
	return &Replication{
		Parameters: params,
	}
}

func (r Replication) Start() {

}
