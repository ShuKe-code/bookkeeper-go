package bookkeepergo

import (
	"sync"
	"sync/atomic"
	"syscall"
)

const (
	DEFAULT_MAX_SEGMENT_SIZE = 1 * 1024 * 1024 * 1024
)

type readCache struct {
	cacheSegments        [][]byte
	cacheIndexes         []*LongLongMap
	currentSegmentIdx    int
	currentSegmentOffset atomic.Int64
	segmentSize          int
	mu                   sync.RWMutex
}

func newReadCache(maxCacheSize, maxSegmentSize int64) (*readCache, error) {
	segmentsCount := 2
	if maxCacheSize/maxSegmentSize > 2 {
		segmentsCount = int(maxCacheSize / maxSegmentSize)
	}
	segmentSize := int(maxCacheSize) / segmentsCount
	cacheSegments := make([][]byte, segmentsCount)
	cacheIndexes := make([]*LongLongMap, segmentsCount)
	for i := 0; i < segmentsCount; i++ {
		data, err := syscall.Mmap(
			-1, 0, int(segmentSize),
			syscall.PROT_READ|syscall.PROT_WRITE,
			syscall.MAP_ANON|syscall.MAP_PRIVATE,
		)
		if err != nil {
			return nil, err
		}
		cacheSegments[i] = data
		cacheIndexes[i] = &LongLongMap{}
	}
	return &readCache{
		cacheSegments:        cacheSegments,
		cacheIndexes:         cacheIndexes,
		currentSegmentIdx:    0,
		currentSegmentOffset: atomic.Int64{},
		segmentSize:          int(segmentSize),
	}, nil
}

func (rc *readCache) Close() {
	for _, segment := range rc.cacheSegments {
		syscall.Munmap(segment)
	}
}

func (rc *readCache) put(ledgerId, entryId int64, entry []byte) {
	entrySize := len(entry)
	alignedSize := align64(int64(entrySize))

	rc.mu.RLock()
	if entrySize > rc.segmentSize {
		log.Warn("Entry size %d is larger than cache segment size %d", entrySize, rc.segmentSize)
		rc.mu.RUnlock()
		return
	}
	offset := rc.currentSegmentOffset.Add(alignedSize)
	if offset+int64(entrySize) > int64(rc.segmentSize) {

	} else {
		copy(rc.cacheSegments[rc.currentSegmentIdx][offset:offset+int64(entrySize)], entry)
		rc.cacheIndexes[rc.currentSegmentIdx].Set(ledgerId, entryId, offset, int64(entrySize))
		rc.mu.RUnlock()
		return
	}
	rc.mu.RUnlock()

	rc.mu.Lock()
	offset = rc.currentSegmentOffset.Add(alignedSize)
	if offset+int64(entrySize) > int64(rc.segmentSize) {
		// Rollover to next segment
		rc.currentSegmentIdx = (rc.currentSegmentIdx + 1) % len(rc.cacheSegments)
		rc.currentSegmentOffset.Store(alignedSize)
		offset = 0
	}
	// Copy entry into read cache segment
	copy(rc.cacheSegments[rc.currentSegmentIdx][offset:offset+int64(entrySize)], entry)
	rc.cacheIndexes[rc.currentSegmentIdx].Set(ledgerId, entryId, offset, int64(entrySize))
	rc.mu.Unlock()
}

func (rc *readCache) get(ledgerId, entryId int64) ([]byte, bool) {
	rc.mu.RLock()
	// We need to check all the segments, starting from the current one and looking
	// backward to minimize the
	// checks for recently inserted entries
	size := len(rc.cacheSegments)
	for i := 0; i < size; i++ {
		segmentIdx := (rc.currentSegmentIdx + (size - i)) % size
		offset, size, ok := rc.cacheIndexes[segmentIdx].Get(ledgerId, entryId)
		if ok {
			entry := make([]byte, size)
			copy(entry, rc.cacheSegments[segmentIdx][offset:offset+int64(size)])
			rc.mu.RUnlock()
			return entry, true
		}
	}
	rc.mu.RUnlock()
	return nil, false
}

func (rc *readCache) hasEntry(ledgerId, entryId int64) bool {
	rc.mu.RLock()
	size := len(rc.cacheSegments)
	for i := 0; i < size; i++ {
		segmentIdx := (rc.currentSegmentIdx + (size - i)) % size
		_, _, ok := rc.cacheIndexes[segmentIdx].Get(ledgerId, entryId)
		if ok {
			rc.mu.RUnlock()
			return true
		}
	}
	rc.mu.RUnlock()
	return false
}

func (rc *readCache) size() int64 {
	rc.mu.RLock()
	var res int64
	size := len(rc.cacheSegments)
	for i := 0; i < size; i++ {
		if i == rc.currentSegmentIdx {
			res += rc.currentSegmentOffset.Load()
		} else if !rc.cacheIndexes[i].IsEmpty() {
			res += int64(rc.segmentSize)
		}
	}
	rc.mu.RUnlock()
	return res
}

func (rc *readCache) count() int64 {
	rc.mu.RLock()
	var res int64
	size := len(rc.cacheSegments)
	for i := 0; i < size; i++ {
		res += int64(rc.cacheIndexes[i].Len())
	}
	rc.mu.RUnlock()
	return res
}
