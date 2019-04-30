package sgreplicate

import (
	"github.com/couchbase/clog"
)

const logKey = "Replicate"

func (r *Replication) log(level clog.LogLevel, format string, args ...interface{}) {
	log(r.Parameters, level, format, args)
}

func (r *ContinuousReplication) log(level clog.LogLevel, format string, args ...interface{}) {
	log(r.Parameters, level, format, args)
}

func log(param ReplicationParameters, level clog.LogLevel, format string, args ...interface{}) {
	if param.ReplicationId != "" {
		format = "[" + param.ReplicationId + "] " + format
	}
	param.LogFn(level, format, args...)
}

func defaultLogFn(level clog.LogLevel, format string, args ...interface{}) {
	if clog.GetLevel() <= level {
		switch level {
		case clog.LevelDebug:
			clog.Debugf(format, args...)
		case clog.LevelNormal:
			clog.To(logKey, format, args...)
		case clog.LevelWarning:
			clog.Warnf(format, args...)
		case clog.LevelError:
			clog.Errorf(format, args...)
		case clog.LevelPanic:
			clog.Panicf(format, args...)
		}
	}
}

type LogFn func(level clog.LogLevel, format string, args ...interface{})
