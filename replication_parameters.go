package sgreplicate

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/couchbase/clog"
)

type ReplicationLifecycle int

const (
	ONE_SHOT = ReplicationLifecycle(iota)
	CONTINUOUS
)

func (l *ReplicationLifecycle) UnmarshalJSON(data []byte) error {

	var s string
	error := json.Unmarshal(data, &s)
	clog.To("Replicate", "replication lifecycle string: %v", s)
	if error == nil {
		switch s {
		case "oneshot":
			*l = ONE_SHOT
		case "continuous":
			*l = CONTINUOUS
		}
	}
	return error

}

const DefaultChangesFeedLimit = 50

type ReplicationParameters struct {
	ReplicationId    string
	Source           *url.URL
	SourceDb         string
	Channels         []string
	Target           *url.URL
	TargetDb         string
	ChangesFeedLimit int
	Lifecycle        ReplicationLifecycle
	Disabled         bool
	Async            bool
}

func (rp ReplicationParameters) GetSourceDbUrl() string {
	return fmt.Sprintf("%s/%s", rp.Source, rp.SourceDb)
}

func (rp ReplicationParameters) GetTargetDbUrl() string {
	return fmt.Sprintf("%s/%s", rp.Target, rp.TargetDb)
}

func (rp ReplicationParameters) Equals(other ReplicationParameters) bool {
	if rp.GetSourceDbUrl() != other.GetSourceDbUrl() {
		return false
	}
	if rp.GetTargetDbUrl() != other.GetTargetDbUrl() {
		return false
	}
	if rp.Lifecycle != other.Lifecycle {
		return false
	}
	return true
}

func (rp ReplicationParameters) getSourceChangesFeedUrl(p ChangesFeedParams) string {
	dbUrl := rp.GetSourceDbUrl()
	changesFeedUrl := fmt.Sprintf(
		"%s/_changes?%s",
		dbUrl,
		p.String(),
	)
	return changesFeedUrl

}
