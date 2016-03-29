package sgreplicate

import (
	"encoding/json"
	"testing"

	"github.com/couchbaselabs/go.assert"
	"github.com/couchbaselabs/logg"
)

func init() {
	logg.LogKeys["TEST"] = true
	logg.LogKeys["Replicate"] = true
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
	logg.LogTo("TEST", "revsDiffMap: %v", revsDiffMap)
}
