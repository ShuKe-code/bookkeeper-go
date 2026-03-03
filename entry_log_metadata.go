package bookkeepergo

import (
	"sync"
	"sync/atomic"
)

type EntryLogMetadata struct {
	entryLogId    int64
	totalSize     int64
	remainingSize int64
	ledgersMap    sync.Map
}

func (meta *EntryLogMetadata) addLedgerSize(ledgerId, size int64) {
	meta.totalSize += size
	actual, _ := meta.ledgersMap.LoadOrStore(ledgerId, new(int64))
	ptr := actual.(*int64)
	atomic.AddInt64(ptr, size)
}

func (meta *EntryLogMetadata) containsLedger(ledgerId int64) bool {
	_, ok := meta.ledgersMap.Load(ledgerId)
	return ok
}

func (meta *EntryLogMetadata) getUsage(ledgerId int64) float64 {
	if meta.totalSize == 0 {
		return 0
	}
	return float64(meta.remainingSize) / float64(meta.totalSize)
}

func (meta *EntryLogMetadata) isEmpty() bool {
	return meta.totalSize == 0
}

type filter func(ledgerId int64) bool

func (meta *EntryLogMetadata) removeLedgerIf(f filter) {
	meta.ledgersMap.Range(func(key, value interface{}) bool {
		ledgerId := key.(int64)
		if f(ledgerId) {
			meta.remainingSize -= *value.(*int64)
			meta.ledgersMap.Delete(ledgerId)
		}
		return true
	})
}
