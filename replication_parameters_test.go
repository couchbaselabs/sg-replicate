package sgreplicate

import (
	"testing"

	assert "github.com/couchbaselabs/go.assert"
)

func TestReplicationParametersEquals(t *testing.T) {
	var tests = []struct {
		name string
		expected bool
		a, b     ReplicationParameters
	}{
		{
			"ReplicationId not equal",
			true, // Equality not checked
			ReplicationParameters{ReplicationId: "r1"},
			ReplicationParameters{ReplicationId: "r2"},
		},
		{
			"ChangesFeedLimit not equal",
			true, // Equality not checked
			ReplicationParameters{ChangesFeedLimit: 100},
			ReplicationParameters{ChangesFeedLimit: 101},
		},
		{
			"Async not equal",
			true, // Equality not checked
			ReplicationParameters{Async: true},
			ReplicationParameters{Async: false},
		},
		{
			"Disabled not equal",
			true, // Equality not checked
			ReplicationParameters{Disabled: true},
			ReplicationParameters{Disabled: false},
		},
		{
			"Channels equal - different order",
			true,
			ReplicationParameters{Channels: []string{"chanA", "chanB"}},
			ReplicationParameters{Channels: []string{"chanB", "chanA"}},
		},
		{
			"Channels not equal",
			false,
			ReplicationParameters{Channels: []string{"chanA", "chanC"}},
			ReplicationParameters{Channels: []string{"chanB", "chanC"}},
		},
		{
			"Channels not equal - missing",
			false,
			ReplicationParameters{Channels: []string{"chanA"}},
			ReplicationParameters{Channels: []string{"chanA", "chanB"}},
		},
		{
			"SourceDB equal",
			true,
			ReplicationParameters{SourceDb: "db"},
			ReplicationParameters{SourceDb: "db"},
		},
		{
			"SourceDB not equal",
			false,
			ReplicationParameters{SourceDb: "db1"},
			ReplicationParameters{SourceDb: "db2"},
		},
		{
			"TargetDb equal",
			true,
			ReplicationParameters{TargetDb: "db"},
			ReplicationParameters{TargetDb: "db"},
		},
		{
			"TargetDb not equal",
			false,
			ReplicationParameters{TargetDb: "db1"},
			ReplicationParameters{TargetDb: "db2"},
		},
		{
			"Lifecycle equal",
			true,
			ReplicationParameters{Lifecycle: ONE_SHOT},
			ReplicationParameters{Lifecycle: ONE_SHOT},
		},
		{
			"Lifecycle not equal",
			false,
			ReplicationParameters{Lifecycle: ONE_SHOT},
			ReplicationParameters{Lifecycle: CONTINUOUS},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(ts *testing.T) {
			assert.Equals(ts, test.a.Equals(test.b), test.expected)
		})
	}
}
