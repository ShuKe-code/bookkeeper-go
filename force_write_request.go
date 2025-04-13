package bookkeepergo

import (
	"container/list"
	"sync"
)

var forceWriteRequestPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &ForceWriteRequest{}
	},
}

type ForceWriteRequest struct {
	flushed             bool
	logFile             *journalChannel
	shouldClose         bool
	lastFlushedPosition uint64
	logId               int64
	forceWriteWaiters   *list.List
}

func NewForceWriteRequest(logFile *journalChannel, logId int64, shouldClose bool,
	lastFlushedPosition uint64, forceWriteWaiters *list.List) *ForceWriteRequest {
	fwr := forceWriteRequestPool.Get().(*ForceWriteRequest)
	fwr.flushed = false
	fwr.logFile = logFile
	fwr.shouldClose = shouldClose
	fwr.lastFlushedPosition = lastFlushedPosition
	fwr.logId = logId
	fwr.forceWriteWaiters = forceWriteWaiters
	return fwr
}

func (fwr *ForceWriteRequest) flushFileToDisk() {
	if !fwr.flushed {
		fwr.logFile.forceWrite(false)
		fwr.flushed = true
	}
}

func (fwr *ForceWriteRequest) closeFileIfNecessary() {
	if fwr.shouldClose {
		fwr.flushFileToDisk()
		fwr.logFile.close()
		fwr.shouldClose = false
	}
}

func (fwr *ForceWriteRequest) process(writeHandlers list.List) int {
	fwr.closeFileIfNecessary()
	for e := fwr.forceWriteWaiters.Front(); e != nil; e = e.Next() {
		if e.Value != nil {
			writeHandlers.PushBack(e.Value)
			e.Value.(*queueEntry).run()
		}
	}
	return fwr.forceWriteWaiters.Len()
}

func (fwr *ForceWriteRequest) release() {
	forceWriteRequestPool.Put(fwr)
}
