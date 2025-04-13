package bookkeepergo

import (
	"sync"
	"time"
)

var queueEntryPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &queueEntry{}
	},
}

type queueEntry struct {
	ledgerId      uint64
	entryId       uint64
	entry         []byte
	enqueueTime   int64
	ackBeforeSync bool
	cb            WriteCallback
}

func NewQueueEntry(ledgerId uint64, entryId uint64, entry []byte,
	enqueueTime int64, ackBeforeSync bool, cb WriteCallback) *queueEntry {
	qe := queueEntryPool.Get().(*queueEntry)
	qe.ledgerId = ledgerId
	qe.entryId = entryId
	qe.entry = entry
	qe.enqueueTime = enqueueTime
	qe.ackBeforeSync = ackBeforeSync
	qe.cb = cb
	return qe
}

func (qe *queueEntry) run() {
	// log.Debug("Acknowledge Ledger: %d, Entry: %d", qe.ledgerId, qe.entryId)
	elapsedNanos := time.Now().UnixNano() - qe.enqueueTime
	journalAddEntryStats.WithLabelValues("true").Observe(float64(elapsedNanos))
	qe.cb.writeComplete(0, qe.ledgerId, qe.entryId)
	qe.release()
}

func (qe *queueEntry) release() {
	queueEntryPool.Put(qe)
}
