package sgreplicate

import (
	"sync"
	"sync/atomic"
)

type ReplicationStats struct {
	lock                       sync.RWMutex
	docsRead                   uint32
	docsWritten                uint32
	docWriteFailures           uint32
	startLastSeq               uint32
	numAttachmentsTransferred  uint64
	attachmentBytesTransferred uint64
	docsCheckedSent            uint64
	active                     int32 // atomic bool
	endLastSeq                 interface{}
}

func (rs *ReplicationStats) GetNumAttachmentsTransferred() uint64 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.numAttachmentsTransferred
}

func (rs *ReplicationStats) AddNumAttachmentsTransferred(delta uint64) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.numAttachmentsTransferred += delta
}

func (rs *ReplicationStats) GetAttachmentBytesTransferred() uint64 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.attachmentBytesTransferred
}

func (rs *ReplicationStats) AddAttachmentBytesTransferred(delta uint64) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.attachmentBytesTransferred += delta
}

func (rs *ReplicationStats) GetDocsCheckedSent() uint64 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.docsCheckedSent
}

func (rs *ReplicationStats) AddDocsCheckedSent(delta uint64) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.docsCheckedSent += delta
}

func (rs *ReplicationStats) GetDocsRead() uint32 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.docsRead
}

func (rs *ReplicationStats) AddDocsRead(addDocsRead uint32) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.docsRead += addDocsRead
}

func (rs *ReplicationStats) GetDocsWritten() uint32 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.docsWritten
}

func (rs *ReplicationStats) AddDocsWritten(addDocsWritten uint32) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.docsWritten += addDocsWritten
}

func (rs *ReplicationStats) GetDocWriteFailures() uint32 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.docWriteFailures
}

func (rs *ReplicationStats) AddDocWriteFailures(addDocWriteFailures uint32) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.docWriteFailures += addDocWriteFailures
}

func (rs *ReplicationStats) GetStartLastSeq() uint32 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.startLastSeq
}

func (rs *ReplicationStats) SetStartLastSeq(updatedStartLastSeq uint32) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.startLastSeq = updatedStartLastSeq
}

func (rs *ReplicationStats) GetActive() bool {
	return atomic.LoadInt32(&rs.active) == 1
}

func (rs *ReplicationStats) SetActive(val bool) {
	if val {
		atomic.StoreInt32(&rs.active, 1)
	} else {
		atomic.StoreInt32(&rs.active, 0)
	}
}

func (rs *ReplicationStats) GetEndLastSeq() interface{} {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.endLastSeq
}

func (rs *ReplicationStats) SetEndLastSeq(updatedEndLastSeq interface{}) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.endLastSeq = updatedEndLastSeq
}
