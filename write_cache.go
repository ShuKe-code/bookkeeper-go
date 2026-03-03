package bookkeepergo

import (
	"errors"
	"fmt"
	"math/bits"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
)

const (
	ALIGN_64_MASK = ^(64 - 1)
)

var bytesPool = sync.Pool{
	New: func() any {
		return make([]byte, 64)
	},
}

type writeCache struct {
	segmentsCount     int
	maxCacheSize      int64
	maxSegmentSize    int
	segmentOffsetMask int64
	segmentOffsetBits int64
	cacheSize         atomic.Int64
	cacheOffset       atomic.Int64
	cacheCount        int64
	deletedLedgers    mapset.Set[int64]
	cacheSegments     [][]byte
	lastEntryMap      sync.Map
	index             *LongLongMap
	sortedEntriesLock sync.Mutex
	sortedEntries     []entryItem
}

func newWriteCache(maxCacheSize int64, maxSegmentSize int) (*writeCache, error) {
	alignedMaxSegmentSize := nextPowerOfTwo(maxSegmentSize)
	if alignedMaxSegmentSize != maxSegmentSize {
		return nil, errors.New("max segment size needs to be in form of 2^n")
	}
	segmentsCount := int(maxCacheSize)/maxSegmentSize + 1
	segmentOffsetBits := 64 - int64(bits.LeadingZeros64(uint64(maxSegmentSize)-1))
	var cacheSegments = make([][]byte, segmentsCount)
	deletedLedgers := mapset.NewSet(int64(0))
	for i := 0; i < segmentsCount-1; i++ {
		data, err := syscall.Mmap(
			-1, 0, int(alignedMaxSegmentSize),
			syscall.PROT_READ|syscall.PROT_WRITE,
			syscall.MAP_ANON|syscall.MAP_PRIVATE,
		)
		if err != nil {
			return nil, err
		}
		cacheSegments[i] = data
	}
	lastSegmentSize := int(maxCacheSize) % maxSegmentSize
	data, err := syscall.Mmap(
		-1, 0, int(lastSegmentSize),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_ANON|syscall.MAP_PRIVATE,
	)
	if err != nil {
		return nil, err
	}
	cacheSegments[segmentsCount-1] = data
	return &writeCache{
		deletedLedgers:    deletedLedgers,
		cacheSegments:     cacheSegments,
		segmentsCount:     segmentsCount,
		maxCacheSize:      maxCacheSize,
		maxSegmentSize:    maxSegmentSize,
		segmentOffsetMask: int64(maxSegmentSize) - 1,
		segmentOffsetBits: segmentOffsetBits,
		index:             &LongLongMap{},
	}, nil
}

func (wc *writeCache) Close() {
	for _, segment := range wc.cacheSegments {
		syscall.Munmap(segment)
	}
}

func (wc *writeCache) put(ledgerId, entryId int64, entry []byte) bool {
	size := len(entry)
	alignedSize := align64(int64(size))

	var (
		offset             int64
		localOffset        int
		segmentIdx         int
		value              any
		currentLastEntryId *int64
		ok                 bool
	)

	for {
		offset = wc.cacheOffset.Add(alignedSize)
		localOffset = int(offset & wc.segmentOffsetMask)
		segmentIdx = int(offset >> wc.segmentOffsetBits)

		if offset+int64(size) > wc.maxCacheSize {
			return false
		} else if wc.maxSegmentSize-localOffset < size {
			// If an entry is at the end of a segment, we need to get a new offset and try
			// again in next segment
			continue
		} else {
			break
		}
	}
	copy(wc.cacheSegments[segmentIdx][localOffset:localOffset+size], entry)

	// Update last entryId for ledger. This logic is to handle writes for the same
	// ledger coming out of order and from different thread, though in practice it
	// should not happen and the compareAndSet should be always uncontended.
	for {
		value, ok = wc.lastEntryMap.LoadOrStore(ledgerId, new(int64))
		if ok {
			currentLastEntryId = value.(*int64)
			if *currentLastEntryId > entryId {
				break
			}
			if atomic.CompareAndSwapInt64(currentLastEntryId, *currentLastEntryId, entryId) {
				break
			}
		}
	}
	wc.index.Set(ledgerId, entryId, offset, int64(size))
	wc.cacheSize.Add(alignedSize)
	atomic.AddInt64(&wc.cacheCount, 1)
	return true
}

func (wc *writeCache) get(ledgerId, entryId int64) ([]byte, bool) {
	offset, size, ok := wc.index.Get(ledgerId, entryId)
	if !ok {
		return nil, false
	}
	data := wc.cacheSegments[int(offset>>wc.segmentOffsetBits)]
	entry := make([]byte, size)
	copy(entry, data[offset&wc.segmentOffsetMask:offset&wc.segmentOffsetMask+int64(size)])
	return entry, true
}

func (wc *writeCache) hasEntry(ledgerId, entryId int64) bool {
	_, _, ok := wc.index.Get(ledgerId, entryId)
	return ok
}

func (wc *writeCache) getLastEntry(ledgerId int64) ([]byte, bool) {
	value, ok := wc.lastEntryMap.Load(ledgerId)
	if !ok {
		return nil, false
	}
	return wc.get(ledgerId, *value.(*int64))
}

func (wc *writeCache) deleteLedger(ledgerId int64) {
	wc.deletedLedgers.Add(ledgerId)
}

type EntryConsumer func(ledgerId, entryId int64, entry []byte)

type entryItem struct {
	ledgerId int64
	entryId  int64
	size     int64
	offset   int64
}

func (wc *writeCache) forEach(consmuer EntryConsumer) {
	wc.sortedEntriesLock.Lock()

	entriesToSort := wc.index.Len()
	arrayLen := entriesToSort * 4
	if len(wc.sortedEntries) < arrayLen {
		wc.sortedEntries = make([]entryItem, arrayLen*2)
	}
	startTime := time.Now()
	sortedEntriesIdx := 0

	wc.index.ForEach(func(k1, k2 int64, v1, v2 int64) {
		if wc.deletedLedgers.Contains(k1) {
			return
		}
		wc.sortedEntries[sortedEntriesIdx].ledgerId = k1
		wc.sortedEntries[sortedEntriesIdx].entryId = k2
		wc.sortedEntries[sortedEntriesIdx].size = v1
		wc.sortedEntries[sortedEntriesIdx].offset = v2
		sortedEntriesIdx++
	})
	log.Debug("iteration took %v", time.Since(startTime))

	startTime = time.Now()
	sort.Slice(wc.sortedEntries[:sortedEntriesIdx], func(i, j int) bool {
		if wc.sortedEntries[i].ledgerId != wc.sortedEntries[j].ledgerId {
			return wc.sortedEntries[i].ledgerId < wc.sortedEntries[j].ledgerId
		}
		return wc.sortedEntries[i].entryId < wc.sortedEntries[j].entryId
	})

	log.Debug("sort took %v", time.Since(startTime))

	startTime = time.Now()
	for i := 0; i < sortedEntriesIdx; i++ {
		localOffset := wc.sortedEntries[i].offset & wc.segmentOffsetMask
		segmentIdx := int(wc.sortedEntries[i].offset >> wc.segmentOffsetBits)
		entry := wc.cacheSegments[segmentIdx][localOffset : localOffset+wc.sortedEntries[i].size]
		consmuer(wc.sortedEntries[i].ledgerId, wc.sortedEntries[i].entryId, entry)
	}

	log.Debug("entry log adding %d ms", time.Since(startTime).Milliseconds())
	wc.sortedEntriesLock.Unlock()
}

func nextPowerOfTwo(n int) int {
	if n == 0 {
		return 1
	}
	return 1 << (64 - bits.LeadingZeros64(uint64(n)-1))
}

func align64(size int64) int64 {
	return (size + 64 - 1) & ALIGN_64_MASK
}

type LongLong struct {
	First  int64
	Second int64
}

type LongLongMap struct {
	m sync.Map
}

func (m *LongLongMap) encodeKey(k1, k2 int64) string {
	return fmt.Sprintf("%d:%d", k1, k2)
}

func (m *LongLongMap) DecodeKey(key string) (int64, int64) {
	parts := strings.Split(key, ":")
	k1, _ := strconv.ParseInt(parts[0], 10, 64)
	k2, _ := strconv.ParseInt(parts[1], 10, 64)
	return k1, k2
}

func (m *LongLongMap) Set(k1, k2, v1, v2 int64) {
	m.m.Store(m.encodeKey(k1, k2), LongLong{v1, v2})
}

func (m *LongLongMap) Get(k1, k2 int64) (int64, int64, bool) {
	val, ok := m.m.Load(m.encodeKey(k1, k2))
	if !ok {
		return 0, 0, false
	}
	ll := val.(LongLong)
	return ll.First, ll.Second, true
}

func (m *LongLongMap) Len() int {
	var cnt int
	m.m.Range(func(key, value interface{}) bool {
		cnt++
		return true
	})
	return cnt
}

func (m *LongLongMap) ForEach(consumer func(k1, k2, v1, v2 int64)) {
	m.m.Range(func(key, value interface{}) bool {
		ll := value.(LongLong)
		k1, k2 := m.DecodeKey(key.(string))
		consumer(k1, k2, ll.First, ll.Second)
		return true
	})
}

func (m *LongLongMap) IsEmpty() bool {
	return m.Len() == 0
}
