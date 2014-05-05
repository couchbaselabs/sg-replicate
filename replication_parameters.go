package synctube

import (
	"encoding/json"
	"fmt"
	"github.com/couchbaselabs/logg"
	"net/url"
)

type ReplicationLifecycle int

const (
	ONE_SHOT_SINGLE_PASS = ReplicationLifecycle(iota)
	ONE_SHOT_MULTI_PASS
	CONTINUOUS
)

func (l *ReplicationLifecycle) UnmarshalJSON(data []byte) error {

	var s string
	error := json.Unmarshal(data, &s)
	logg.LogTo("SYNCTUBE", "replciation lifecycle string: %v", s)
	if error == nil {
		switch s {
		case "oneshot":
			fallthrough
		case "oneshot:multipass":
			*l = ONE_SHOT_MULTI_PASS
		case "oneshot:singlepass":
			*l = ONE_SHOT_SINGLE_PASS
		case "continuous":
			*l = CONTINUOUS
		}
	}
	return error

}

const DefaultChangesFeedLimit = 50

type ReplicationParameters struct {
	Name             string
	Source           *url.URL
	SourceDb         string
	Target           *url.URL
	TargetDb         string
	ChangesFeedLimit int
	Lifecycle        ReplicationLifecycle
}

func (rp ReplicationParameters) getSourceDbUrl() string {
	return fmt.Sprintf("%s/%s", rp.Source, rp.SourceDb)
}

func (rp ReplicationParameters) getTargetDbUrl() string {
	return fmt.Sprintf("%s/%s", rp.Target, rp.TargetDb)
}

func (rp ReplicationParameters) getSourceChangesFeedUrl(p ChangesFeedParams) string {
	dbUrl := rp.getSourceDbUrl()
	changesFeedUrl := fmt.Sprintf(
		"%s/_changes?%s",
		dbUrl,
		p.String(),
	)
	return changesFeedUrl

}
