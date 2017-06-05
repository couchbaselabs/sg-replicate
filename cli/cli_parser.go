package main

import (
	"encoding/json"
	"io"
	"net/url"

	sgreplicate "github.com/couchbaselabs/sg-replicate"
	"log"
)

type ReplicationsConfig struct {
	ChangesFeedLimit      int
	ContinuousRetryTimeMs int
	Replications          []sgreplicate.ReplicationParameters
}

type ReplicationsConfigJson struct {
	ContinuousRetryTimeMs int                                  `json:"continuous_retry_time_ms"`
	ChangesFeedLimit      *int                                 `json:"changes_feed_limit"`
	ReplicationsMap       map[string]ReplicationParametersJson `json:"replications"`
}

type ReplicationParametersJson struct {
	Source           string                           `json:"source_url"`
	SourceDb         string                           `json:"source_db"`
	Target           string                           `json:"target_url"`
	TargetDb         string                           `json:"target_db"`
	Channels         []string                         `json:"channels"`
	ChangesFeedLimit *int                             `json:"changes_feed_limit"`
	Lifecycle        sgreplicate.ReplicationLifecycle `json:"lifecycle"`
	Disabled         bool                             `json:"disabled"`
}

func (r ReplicationsConfigJson) Export() (ReplicationsConfig, error) {

	result := ReplicationsConfig{}

	if r.ChangesFeedLimit != nil {
		result.ChangesFeedLimit = *r.ChangesFeedLimit
	} else {
		result.ChangesFeedLimit = sgreplicate.DefaultChangesFeedLimit
	}

	result.ContinuousRetryTimeMs = r.ContinuousRetryTimeMs

	for k, replicationParametersJson := range r.ReplicationsMap {

		if replicationParams, err := replicationParametersJson.Export(); err != nil {
			return result, err
		} else {
			replicationParams.ReplicationId = k

			// If there ChangesFeedLimit wasn't specified in the JSON in the per-replication config
			// section, then default to the value specified at the top level of the config
			if replicationParametersJson.ChangesFeedLimit == nil {
				replicationParams.ChangesFeedLimit = result.ChangesFeedLimit
			}

			result.Replications = append(result.Replications, replicationParams)
		}
	}

	return result, nil
}

func (p ReplicationParametersJson) Export() (sgreplicate.ReplicationParameters, error) {

	result := sgreplicate.ReplicationParameters{}
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

	if p.ChangesFeedLimit != nil {
		result.ChangesFeedLimit = *p.ChangesFeedLimit
	} else {
		result.ChangesFeedLimit = sgreplicate.DefaultChangesFeedLimit
	}

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
