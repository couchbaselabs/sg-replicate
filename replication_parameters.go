package synctube

import (
	"fmt"
	"net/url"
)

const DefaultChangesFeedLimit = 50

type ReplicationParameters struct {
	Source           *url.URL
	SourceDb         string
	Target           *url.URL
	TargetDb         string
	Continuous       bool
	ChangesFeedLimit int
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
		"%s/_changes?feed=%s&limit=%s&heartbeat=%s&style=%s",
		dbUrl,
		p.FeedType(),
		p.Limit(),
		p.HeartbeatTimeMillis(),
		p.FeedStyle(),
	)
	return changesFeedUrl

}
