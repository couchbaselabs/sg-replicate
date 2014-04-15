package main

import (
	"github.com/couchbaselabs/go.assert"
	"github.com/couchbaselabs/logg"
	"strings"
	"testing"
)

func init() {
	logg.LogKeys["TEST"] = true
	logg.LogKeys["SYNCTUBE"] = true
}

func TestParseConfig(t *testing.T) {

	config := `{
    "changes_feed_limit": 50,
    "replications":{
	"checkers":{
	    "source_url": "http://checkers.sync.couchbasecloud.com",
	    "source_db": "checkers",
	    "target_db": "checkers-copy"
	} 
    }

}`
	logg.LogTo("TEST", "config: %v", config)
	reader := strings.NewReader(config)
	replicationsConfig, err := ParseReplicationsConfig(reader)
	logg.LogTo("TEST", "err: %v", err)
	assert.True(t, err == nil)
	assert.Equals(t, len(replicationsConfig.Replications), 1)

}
