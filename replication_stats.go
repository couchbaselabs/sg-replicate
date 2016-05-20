package sgreplicate

import "sync"

type ReplicationStats struct {
	lock             sync.Mutex
	docsRead         uint32
	docsWritten      uint32
	docWriteFailures uint32
	startLastSeq     uint32
	endLastSeq       interface{}
}

func (rs *ReplicationStats) GetDocsRead() uint32 {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	return rs.docsRead
}

func (rs *ReplicationStats) AddDocsRead(addDocsRead uint32) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.docsRead += addDocsRead
}

func (rs *ReplicationStats) GetDocsWritten() uint32 {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	return rs.docsWritten
}

func (rs *ReplicationStats) AddDocsWritten(addDocsWritten uint32) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.docsWritten += addDocsWritten
}

func (rs *ReplicationStats) GetDocWriteFailures() uint32 {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	return rs.docWriteFailures
}

func (rs *ReplicationStats) AddDocWriteFailures(addDocWriteFailures uint32) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.docWriteFailures += addDocWriteFailures
}

func (rs *ReplicationStats) GetStartLastSeq() uint32 {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	return rs.startLastSeq
}

func (rs *ReplicationStats) SetStartLastSeq(updatedStartLastSeq uint32) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.startLastSeq = updatedStartLastSeq
}

func (rs *ReplicationStats) GetEndLastSeq() interface{} {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	return rs.endLastSeq
}

func (rs *ReplicationStats) SetEndLastSeq(updatedEndLastSeq interface{}) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.endLastSeq = updatedEndLastSeq
}
