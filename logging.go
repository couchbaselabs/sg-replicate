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

// This returns a function closure bound to this particular replication
// instance, so that it emits the replication id when invoked.
func (lr LoggingReplication) getLoggerFunction() loggerFunction {
	return func(key string, format string, args ...interface{}) {
		clog.To(key, prefixWithReplicationId(lr, format), args...)
	}
}

func prefixWithReplicationId(lr LoggingReplication, raw string) string {

	replicationId := lr.Parameters.ReplicationId

	if replicationId != "" {
		return strings.Join([]string{"[", replicationId, "] ", raw}, "")
	}

	return raw
}

type loggerFunction func(key string, format string, args ...interface{})
