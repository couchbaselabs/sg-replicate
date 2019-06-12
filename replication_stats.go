package sgreplicate

import (
	"sync"
	"sync/atomic"
)

type ReplicationStats struct {
	docsRead                   uint32
	docsWritten                uint32
	docWriteFailures           uint32
	startLastSeq               uint32
	numAttachmentsTransferred  uint64
	attachmentBytesTransferred uint64
	docsCheckedSent            uint64
	endLastSeq                 interface{}
	endLastSeqLock             sync.RWMutex
}

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
