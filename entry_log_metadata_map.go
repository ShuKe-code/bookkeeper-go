package bookkeepergo

import (
	"sync"
)

type EntryLogMetadataMap interface {
	containsKey(ledgerId int64) bool
	put(ledgerId int64, meta *EntryLogMetadata) error
	remove(ledgerId int64) error
	size() int
	isEmpty() bool
	clear()
	forEach(consumer BiConsumer)
	forKey(ledgerId int64, consumer BiConsumer)
}

type InMemoryEntryLogMetadataMap struct {
	entryLogMetaMap sync.Map
}

func (metaMap *InMemoryEntryLogMetadataMap) containsKey(ledgerId int64) bool {
	_, ok := metaMap.entryLogMetaMap.Load(ledgerId)
	return ok
}

func (metaMap *InMemoryEntryLogMetadataMap) put(ledgerId int64, meta *EntryLogMetadata) error {
	metaMap.entryLogMetaMap.Store(ledgerId, meta)
	return nil
}

func (metaMap *InMemoryEntryLogMetadataMap) remove(ledgerId int64) error {
	metaMap.entryLogMetaMap.Delete(ledgerId)
	return nil
}

func (metaMap *InMemoryEntryLogMetadataMap) size() int {
	var cnt int
	metaMap.entryLogMetaMap.Range(func(key, value interface{}) bool {
		cnt++
		return true
	})
	return cnt
}

func (metaMap *InMemoryEntryLogMetadataMap) isEmpty() bool {
	return metaMap.size() == 0
}

func (metaMap *InMemoryEntryLogMetadataMap) clear() {
	metaMap.entryLogMetaMap.Range(func(key, value interface{}) bool {
		metaMap.entryLogMetaMap.Delete(key)
		return true
	})
}

type BiConsumer func(ledgerId int64, meta *EntryLogMetadata)

func (metaMap *InMemoryEntryLogMetadataMap) forEach(consumer BiConsumer) {
	metaMap.entryLogMetaMap.Range(func(key, value interface{}) bool {
		ledgerId := key.(int64)
		meta := value.(*EntryLogMetadata)
		consumer(ledgerId, meta)
		return true
	})
}

func (metaMap *InMemoryEntryLogMetadataMap) forKey(ledgerId int64, consumer BiConsumer) {
	value, ok := metaMap.entryLogMetaMap.Load(ledgerId)
	if ok {
		meta := value.(*EntryLogMetadata)
		consumer(ledgerId, meta)
	}
}
