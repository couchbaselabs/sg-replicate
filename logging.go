package sgreplicate

import (
	"strings"
	"github.com/couchbase/clog"
	"fmt"
)

type LoggingReplication struct {
	Parameters ReplicationParameters
}

func (lr LoggingReplication) LogTo(key string, format string, args ...interface{}) {
	clog.To(key, prefixWithReplicationId(lr, format), args)
}

func (lr LoggingReplication) Warn(args ...interface{}) {
	if clog.Level <= clog.LevelWarning {
		clog.Warn(prefixWithReplicationId(lr, fmt.Sprint(args...)))
	}
}

func prefixWithReplicationId(lr LoggingReplication, raw string) string {

	replicationId := lr.Parameters.ReplicationId

	if replicationId != "" {
		return strings.Join([]string{"[",replicationId ,"] ", raw}, "")
	}

	return raw
}

