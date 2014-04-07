package synctube

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

func TestChangesFeedParametersString(t *testing.T) {

	changesFeedParams := NewChangesFeedParams()
	changesFeedParams.since = *NewSequenceNumber(13)
	stringVal := changesFeedParams.String()
	logg.LogTo("TEST", "stringVal: %v", stringVal)
	assert.True(t, strings.Contains(stringVal, "since=13"))
	assert.False(t, strings.Contains(stringVal, "MISSING"))

}
