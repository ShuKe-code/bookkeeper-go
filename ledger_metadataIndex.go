package bookkeepergo

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/ShuKe-code/bookkeeper-go/proto"
	mapset "github.com/deckarep/golang-set/v2"
)

const (
	STORAGE_FLAGS = -0xeefd
)

type LedgerMetadataIndex struct {
	ledgers               sync.Map
	ledgersCount          atomic.Int64
	ledgersDb             KeyValueStorage
	pendingLedgersUpdates *list.List
	mu                    []sync.Mutex
	data                  proto.LedgerData
	pendingDeletedLedgers mapset.Set[int64]
}

func newLedgerMetadataIndex(cfg *Config, basePath string) (*LedgerMetadataIndex, error) {
	ledgersDb, err := newKeyValueStorageRocksDB(basePath, "ledgers", cfg, LedgerMetadata)
	if err != nil {
		return nil, err
	}

	locks := make([]sync.Mutex, 16)
	for i := 0; i < 16; i++ {
		locks[i] = sync.Mutex{}
	}
	return &LedgerMetadataIndex{
		ledgersDb:             ledgersDb,
		pendingLedgersUpdates: list.New(),
		pendingDeletedLedgers: mapset.NewSet[int64](),
	}, nil
}

func (lmi *LedgerMetadataIndex) lose() {
	lmi.ledgersDb.close()
}

func (lmi *LedgerMetadataIndex) get(ledgerId int64) (*proto.LedgerData, error) {
	value, ok := lmi.ledgers.Load(ledgerId)
	if !ok {
		return nil, nil
	}
	return value.(*proto.LedgerData), nil
}

type simpleEntry struct {
	ledgerId   int64
	ledgerData *proto.LedgerData
}

func (lmi *LedgerMetadataIndex) set(ledgerId int64, ledger *proto.LedgerData) error {
	ledger.Exists = true
	lmi.mu[lmi.lockIdForLedger(ledgerId)].Lock()

	_, ok := lmi.ledgers.Load(ledgerId)
	if !ok {
		lmi.ledgers.Store(ledgerId, ledger)
		lmi.ledgersCount.Add(1)
	} else {
		lmi.ledgers.Store(ledgerId, ledger)
	}
	lmi.pendingLedgersUpdates.PushBack(&simpleEntry{ledgerId: ledgerId, ledgerData: ledger})
	lmi.pendingDeletedLedgers.Remove(ledgerId)
	lmi.mu[lmi.lockIdForLedger(ledgerId)].Unlock()
	return nil
}

func (lmi *LedgerMetadataIndex) lockIdForLedger(ledgerId int64) int {
	return int(ledgerId) % len(lmi.mu)
}

func (lmi *LedgerMetadataIndex) delete(ledgerId int64) {
	lmi.mu[lmi.lockIdForLedger(ledgerId)].Lock()

	_, ok := lmi.ledgers.Load(ledgerId)
	if !ok {
		lmi.ledgers.Delete(ledgerId)
		lmi.ledgersCount.Add(-1)
	}
	lmi.pendingDeletedLedgers.Add(ledgerId)

	for e := lmi.pendingLedgersUpdates.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*simpleEntry)
		if entry.ledgerId == ledgerId {
			lmi.pendingLedgersUpdates.Remove(e)
		}
	}

	lmi.mu[lmi.lockIdForLedger(ledgerId)].Unlock()
}

func (lmi *LedgerMetadataIndex) getActiveLedgersInRange(firstLedgerId, lastLedgerId int64) []int64 {
	ledgers := make([]int64, 0)
	lmi.ledgers.Range(func(key, value interface{}) bool {
		ledgerId := key.(int64)
		if ledgerId >= firstLedgerId && ledgerId <= lastLedgerId {
			ledgers = append(ledgers, ledgerId)
		}
		return true
	})
	return ledgers
}

func (lmi *LedgerMetadataIndex) setFenced(ledgerId int64) bool {
	// ledger.Exists = true
	// lmi.mu[lmi.lockIdForLedger(ledgerId)].Lock()

	// ledgerData, ok := lmi.ledgers.Load(ledgerId)
	// if !ok {
	// 	lmi.ledgers.Store(ledgerId, ledger)
	// 	lmi.ledgersCount.Add(1)
	// }
	// lmi.pendingLedgersUpdates.PushBack(&simpleEntry{ledgerId: ledgerId, ledgerData: ledger})
	// lmi.pendingDeletedLedgers.Remove(ledgerId)
	// lmi.mu[lmi.lockIdForLedger(ledgerId)].Unlock()
	// return nil
	return false
}
