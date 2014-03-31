package synctube

import (
	"encoding/json"
	"github.com/couchbaselabs/go.assert"
	"github.com/couchbaselabs/logg"
	"testing"
)

func init() {
	logg.LogKeys["TEST"] = true
	logg.LogKeys["SYNCTUBE"] = true
}

func TestGenerateRevsMap(t *testing.T) {
	fakeChangesFeed := fakeChangesFeed()
	changes := Changes{}
	err := json.Unmarshal([]byte(fakeChangesFeed), &changes)
	assert.True(t, err == nil)
	revsDiffMap := generateRevsDiffMap(changes)
	assert.Equals(t, len(revsDiffMap), 3)
	assert.Equals(t, len(revsDiffMap["doc2"]), 1)
	assert.Equals(t, len(revsDiffMap["doc3"]), 1)
	logg.LogTo("TEST", "revsDiffMap: %v", revsDiffMap)
}

func TestIncrementLocalRevision(t *testing.T) {
	assert.Equals(t, incrementLocalRevision(""), "0-1")
	assert.Equals(t, incrementLocalRevision("0-0"), "0-1")
	assert.Equals(t, incrementLocalRevision("0-1"), "0-2")
}
