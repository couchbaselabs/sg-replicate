package sgreplicate

import (
	"github.com/couchbaselabs/go.assert"
	"github.com/couchbase/clog"
	"strings"
	"testing"
)

func init() {
	clog.EnableKey("TEST")
	clog.EnableKey("Replicate")
}

func TestChangesFeedParametersString(t *testing.T) {

	changesFeedParams := NewChangesFeedParams()
	changesFeedParams.since = 13
	stringVal := changesFeedParams.String()
	clog.To("TEST", "stringVal: %v", stringVal)
	assert.True(t, strings.Contains(stringVal, "since=13"))
	assert.False(t, strings.Contains(stringVal, "MISSING"))

}

func TestChangesFeedParametersStringChannels(t *testing.T) {
	changesFeedParams := NewChangesFeedParams()
	changesFeedParams.since = 13
	changesFeedParams.channels = []string{"ace", "queen", "king"}
	stringVal := changesFeedParams.String()
	clog.To("TEST", "stringVal: %v", stringVal)
	assert.True(t, strings.Contains(stringVal, "since=13"))
	assert.False(t, strings.Contains(stringVal, "MISSING"))
	assert.True(t, strings.Contains(stringVal, "ace,queen,king"))

}
