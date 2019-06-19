package sgreplicate

import (
	"expvar"
	"sync/atomic"
)

// ReplicationStats stores values for the given replication
type ReplicationStats struct {
	DocsRead                   *expvar.Int
	DocsWritten                *expvar.Int
	DocWriteFailures           *expvar.Int
	StartLastSeq               *expvar.Int
	NumAttachmentsTransferred  *expvar.Int
	AttachmentBytesTransferred *expvar.Int
	DocsCheckedSent            *expvar.Int
	Active                     *AtomicBool
	EndLastSeq                 *expvar.String
}

func NewReplicationStats() *ReplicationStats {
	return &ReplicationStats{
		DocsRead:                   &expvar.Int{},
		DocsWritten:                &expvar.Int{},
		DocWriteFailures:           &expvar.Int{},
		StartLastSeq:               &expvar.Int{},
		NumAttachmentsTransferred:  &expvar.Int{},
		AttachmentBytesTransferred: &expvar.Int{},
		DocsCheckedSent:            &expvar.Int{},
		Active:                     &AtomicBool{},
		EndLastSeq:                 &expvar.String{},
	}
}

// AtomicBool is a a boolean that can be atomically set and read, that satisfies the Var interface.
type AtomicBool struct {
	val int32
}

// String satisfies the expvar.Var interface
func (a *AtomicBool) String() string {
	if !a.Get() {
		return "false"
	}
	return "true"
}

// compile-time interface check
var _ expvar.Var = &AtomicBool{}

func (a *AtomicBool) Get() bool {
	return atomic.LoadInt32(&a.val) != 0
}

func (a *AtomicBool) Set(val bool) {
	if val {
		atomic.StoreInt32(&a.val, 1)
	} else {
		atomic.StoreInt32(&a.val, 0)
	}
}
