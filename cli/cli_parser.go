package main

import (
	"encoding/json"
	"io"
	"net/url"

	"github.com/couchbaselabs/logg"
	synctube "github.com/couchbaselabs/sg-replicate"
)

type ReplicationsConfig struct {
	ChangesFeedLimit      int
	ContinuousRetryTimeMs int
	Replications          []synctube.ReplicationParameters
}

type ReplicationsConfigJson struct {
	ContinuousRetryTimeMs int                                  `json:"continuous_retry_time_ms"`
	ChangesFeedLimit      int                                  `json:"changes_feed_limit"`
	ReplicationsMap       map[string]ReplicationParametersJson `json:"replications"`
}

type ReplicationParametersJson struct {
	Source           string                        `json:"source_url"`
	SourceDb         string                        `json:"source_db"`
	Target           string                        `json:"target_url"`
	TargetDb         string                        `json:"target_db"`
	Channels         []string                      `json:"channels"`
	ChangesFeedLimit int                           `json:"changes_feed_limit"`
	Lifecycle        synctube.ReplicationLifecycle `json:"lifecycle"`
	Disabled         bool                          `json:"disabled"`
}

func (r ReplicationsConfigJson) Export() (ReplicationsConfig, error) {
	result := ReplicationsConfig{}
	result.ChangesFeedLimit = r.ChangesFeedLimit
	result.ContinuousRetryTimeMs = r.ContinuousRetryTimeMs
	for k, v := range r.ReplicationsMap {
		if replicationParams, err := v.Export(); err != nil {
			return result, err
		} else {
			replicationParams.Name = k
			result.Replications = append(result.Replications, replicationParams)
		}
	}
	return result, nil
}

func (p ReplicationParametersJson) Export() (synctube.ReplicationParameters, error) {
	result := synctube.ReplicationParameters{}
	sourceUrl, err := url.Parse(p.Source)
	if err != nil {
		return result, err
	}

	result.Source = sourceUrl
	if len(p.Target) == 0 {
		result.Target = sourceUrl
	} else {
		if targetUrl, err := url.Parse(p.Target); err != nil {
			return result, err
		} else {
			result.Target = targetUrl
		}

	}
	result.SourceDb = p.SourceDb
	result.TargetDb = p.TargetDb
	result.Lifecycle = p.Lifecycle
	result.Channels = p.Channels
	result.Disabled = p.Disabled
	return result, nil

}

func ParseReplicationsConfig(r io.Reader) (ReplicationsConfig, error) {

	replicationsConfigJson := ReplicationsConfigJson{}
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(&replicationsConfigJson); err != nil {
		return ReplicationsConfig{}, err
	}
	result, err := replicationsConfigJson.Export()
	if err != nil {
		return ReplicationsConfig{}, err
	}

	return result, nil

}
