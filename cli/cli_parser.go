package main

import (
	"encoding/json"
	"github.com/couchbaselabs/logg"
	"github.com/tleyden/synctube"
	"io"
	"net/url"
)

type ReplicationsConfig struct {
	ChangesFeedLimit int
	Replications     []synctube.ReplicationParameters
}

type ReplicationsConfigJson struct {
	ChangesFeedLimit int                                  `json:"changes_feed_limit"`
	ReplicationsMap  map[string]ReplicationParametersJson `json:"replications"`
}

type ReplicationParametersJson struct {
	Source           string `json:"source_url"`
	SourceDb         string `json:"source_db"`
	Target           string `json:"target_url"`
	TargetDb         string `json:"target_db"`
	ChangesFeedLimit int    `json:"changes_feed_limit"`
}

func (r ReplicationsConfigJson) Export() (ReplicationsConfig, error) {
	result := ReplicationsConfig{}
	result.ChangesFeedLimit = r.ChangesFeedLimit
	for k, v := range r.ReplicationsMap {
		logg.LogTo("SYNCTUBE", "k: %v, v: %v", k, v)
		if replicationParams, err := v.Export(); err != nil {
			return result, err
		} else {
			replicationParams.Name = k
			result.Replications = append(result.Replications, replicationParams)
		}
	}
	logg.LogTo("SYNCTUBE", "result.Replications: %v", result.Replications)
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
	logg.LogTo("SYNCTUBE", "result.Replications: %v", result.Replications)

	return result, nil

}
