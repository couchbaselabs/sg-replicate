package synctube

import (
	"fmt"
	"net/url"
)

type ReplicationParameters struct {
	Source     *url.URL
	SourceDb   string
	Target     *url.URL
	TargetDb   string
	Continuous bool
}

func (params ReplicationParameters) getSourceDbUrl() string {
	return fmt.Sprintf("%s/%s", params.Source, params.SourceDb)
}

func (params ReplicationParameters) getTargetDbUrl() string {
	return fmt.Sprintf("%s/%s", params.Target, params.TargetDb)
}

func (params ReplicationParameters) getChangesFeedType() string {
	return "longpoll"
}

func (params ReplicationParameters) getChangesFeedLimit() string {
	return "50"
}

func (params ReplicationParameters) getChangesFeedHeartbeat() string {
	return "300000"
}

func (params ReplicationParameters) getChangesFeedStyle() string {
	return "all_docs"
}
