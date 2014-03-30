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
	if params.ChangesFeedLimit == 0 {
		return fmt.Sprintf("%v", DefaultChangesFeedLimit)
	} else {
		return fmt.Sprintf("%v", params.ChangesFeedLimit)
	}

}

func (params ReplicationParameters) getChangesFeedHeartbeat() string {
	return "300000"
}

func (params ReplicationParameters) getChangesFeedStyle() string {
	return "all_docs"
}
