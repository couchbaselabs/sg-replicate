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

	clog.Log("LoggingReplication.LogTo called with key: %v, format: %v, args: %v", key, format, args)
	prefixedFormat := prefixWithReplicationId(lr, format)
	clog.Log("LoggingReplication.LogTo prefixedFormat: %v", prefixedFormat)
	clog.To(key, prefixedFormat, args...)
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
