package sgreplicate

import (
	"encoding/json"
	"testing"

	"github.com/couchbaselabs/go.assert"
	"github.com/couchbase/clog"
)

func init() {
	clog.EnableKey("TEST")
	clog.EnableKey("Replicate")
}

func TestGenerateRevsMap(t *testing.T) {
	lastSequence := "3"
	fakeChangesFeed := fakeChangesFeed(lastSequence)
	changes := Changes{}
	err := json.Unmarshal([]byte(fakeChangesFeed), &changes)
	assert.True(t, err == nil)
	revsDiffMap := generateRevsDiffMap(changes)
	assert.Equals(t, len(revsDiffMap), 2)
	assert.Equals(t, len(revsDiffMap["doc2"]), 1)
	assert.Equals(t, len(revsDiffMap["doc3"]), 1)
	clog.To("TEST", "revsDiffMap: %v", revsDiffMap)
}
