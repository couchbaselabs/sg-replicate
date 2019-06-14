package sgreplicate

import (
	"expvar"
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	StatKeySgrActive                     = "sgr_active"
	StatKeySgrNumAttachmentsTransferred  = "sgr_num_attachments_transferred"
	StatKeySgrAttachmentBytesTransferred = "sgr_num_attachment_bytes_transferred"
	StatKeySgrNumDocsFailedToPush        = "sgr_num_docs_failed_to_push"
	StatKeySgrNumDocsPushed              = "sgr_num_docs_pushed"
	StatKeySgrDocsCheckedSent            = "sgr_docs_checked_sent"
)

// ReplicationStats holds atomic values for the given replication, and also satisfies the expvar.Var interface
type ReplicationStats struct {
	docsRead                   uint32
	docsWritten                uint32
	docWriteFailures           uint32
	startLastSeq               uint32
	numAttachmentsTransferred  uint64
	attachmentBytesTransferred uint64
	docsCheckedSent            uint64
	active                     atomicBool
	endLastSeq                 interface{}
	endLastSeqLock             sync.RWMutex
}

// String returns the given ReplicationStats as JSON to satisfy the expvar.Var interface
func (rs *ReplicationStats) String() string {
	return fmt.Sprintf(`{"%s":%t,"%s":%d,"%s":%d,"%s":%d,"%s":%d,"%s":%d}`,
		StatKeySgrActive, rs.GetActive(),
		StatKeySgrNumAttachmentsTransferred, rs.GetNumAttachmentsTransferred(),
		StatKeySgrAttachmentBytesTransferred, rs.GetAttachmentBytesTransferred(),
		StatKeySgrNumDocsFailedToPush, rs.GetDocWriteFailures(),
		StatKeySgrNumDocsPushed, rs.GetDocsWritten(),
		StatKeySgrDocsCheckedSent, rs.GetDocsCheckedSent(),
	)
}

// Compile-time interface check
var _ expvar.Var = &ReplicationStats{}

func (rs *ReplicationStats) GetNumAttachmentsTransferred() uint64 {
	return atomic.LoadUint64(&rs.numAttachmentsTransferred)
}

func (rs *ReplicationStats) AddNumAttachmentsTransferred(delta uint64) {
	atomic.AddUint64(&rs.numAttachmentsTransferred, delta)
}

func (rs *ReplicationStats) GetAttachmentBytesTransferred() uint64 {
	return atomic.LoadUint64(&rs.attachmentBytesTransferred)
}

func (rs *ReplicationStats) AddAttachmentBytesTransferred(delta uint64) {
	atomic.AddUint64(&rs.attachmentBytesTransferred, delta)
}

func (rs *ReplicationStats) GetDocsCheckedSent() uint64 {
	return atomic.LoadUint64(&rs.docsCheckedSent)
}

func (rs *ReplicationStats) AddDocsCheckedSent(delta uint64) {
	atomic.AddUint64(&rs.docsCheckedSent, delta)
}

func (rs *ReplicationStats) GetDocsRead() uint32 {
	return atomic.LoadUint32(&rs.docsRead)
}

func (rs *ReplicationStats) AddDocsRead(addDocsRead uint32) {
	atomic.AddUint32(&rs.docsRead, addDocsRead)
}

func (rs *ReplicationStats) GetDocsWritten() uint32 {
	return atomic.LoadUint32(&rs.docsWritten)
}

func (rs *ReplicationStats) AddDocsWritten(addDocsWritten uint32) {
	atomic.AddUint32(&rs.docsWritten, addDocsWritten)
}

func (rs *ReplicationStats) GetDocWriteFailures() uint32 {
	return atomic.LoadUint32(&rs.docWriteFailures)
}

func (rs *ReplicationStats) AddDocWriteFailures(addDocWriteFailures uint32) {
	atomic.AddUint32(&rs.docWriteFailures, addDocWriteFailures)
}

func (rs *ReplicationStats) GetStartLastSeq() uint32 {
	return atomic.LoadUint32(&rs.startLastSeq)
}

func (rs *ReplicationStats) SetStartLastSeq(updatedStartLastSeq uint32) {
	atomic.StoreUint32(&rs.startLastSeq, updatedStartLastSeq)
}

func (rs *ReplicationStats) GetEndLastSeq() interface{} {
	rs.endLastSeqLock.RLock()
	defer rs.endLastSeqLock.RUnlock()
	return rs.endLastSeq
}

func (rs *ReplicationStats) SetEndLastSeq(updatedEndLastSeq interface{}) {
	rs.endLastSeqLock.Lock()
	defer rs.endLastSeqLock.Unlock()
	rs.endLastSeq = updatedEndLastSeq
}

func (rs *ReplicationStats) GetActive() bool {
	return rs.active.Get()
}

func (rs *ReplicationStats) SetActive(val bool) {
	rs.active.Set(val)
}

type atomicBool struct {
	val int32
}

func (a *atomicBool) Get() bool {
	return atomic.LoadInt32(&a.val) != 0
}

func (a *atomicBool) Set(val bool) {
	if val {
		atomic.StoreInt32(&a.val, 1)
	} else {
		atomic.StoreInt32(&a.val, 0)
	}
}
