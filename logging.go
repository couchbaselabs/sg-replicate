package sgreplicate

import (
	"fmt"
	"strings"

	"github.com/couchbase/clog"
)

type LoggingReplication struct {
	Parameters ReplicationParameters
}

func (lr LoggingReplication) LogTo(key string, format string, args ...interface{}) {
	clog.To(key, prefixWithReplicationId(lr, format), args...)
}

func (lr LoggingReplication) Warn(args ...interface{}) {
	if clog.Level <= clog.LevelWarning {
		clog.Warn(prefixWithReplicationId(lr, fmt.Sprint(args...)))
	}
}

func prefixWithReplicationId(lr LoggingReplication, raw string) string {

	replicationId := lr.Parameters.ReplicationId

	if replicationId != "" {
		return strings.Join([]string{"[", replicationId, "] ", raw}, "")
	}

	return raw
}

// Lighter-weight LogTo that doesn't require to be called as a method.
// Good for functions, at the cost of not logging the replication-id.
func LogTo(key string, format string, args ...interface{}) {
	clog.To(key, format, args...)
}
