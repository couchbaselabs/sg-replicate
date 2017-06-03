package main

import (
	"strings"
	"testing"

	"github.com/couchbase/clog"
	"github.com/couchbaselabs/go.assert"
	sgreplicate "github.com/couchbaselabs/sg-replicate"
)

func init() {
	clog.EnableKey("TEST")
	clog.EnableKey("Replicate")
}

func TestParseConfig(t *testing.T) {

	config := `{
    "changes_feed_limit": 100,
    "replications":{
	"checkers":{
	    "source_url": "http://checkers.sync.couchbasecloud.com",
	    "source_db": "checkers",
	    "target_db": "checkers-copy",
            "lifecycle": "oneshot"
	},
	"checkers-other-direction":{
	    "source_url": "http://checkers.sync.couchbasecloud.com",
	    "target_db": "checkers",
	    "source_db": "checkers-copy",
            "lifecycle": "oneshot",
            "disabled": true
	} 
 
    }

}`

	reader := strings.NewReader(config)
	replicationsConfig, err := ParseReplicationsConfig(reader)
	clog.To("TEST", "err: %v", err)
	assert.True(t, err == nil)
	assert.Equals(t, len(replicationsConfig.Replications), 2)
	assert.Equals(t, replicationsConfig.Replications[0].Lifecycle, sgreplicate.ONE_SHOT)
	assert.Equals(t, replicationsConfig.Replications[0].Disabled, false)
	assert.Equals(t, replicationsConfig.Replications[1].Disabled, true)
	assert.Equals(t, *replicationsConfig.Replications[0].ChangesFeedLimit, 100)
	assert.Equals(t, *replicationsConfig.Replications[1].ChangesFeedLimit, 100)

}
