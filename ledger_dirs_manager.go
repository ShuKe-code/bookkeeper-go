package bookkeepergo

import "sync"

type ledgerDirsManager struct {
	filledDirs                        []string
	ledgerDirectories                 []string
	writableLedgerDirectories         []string
	diskUsages                        sync.Map
	entryLogSize                      int64
	minUsableSizeForEntryLogCreation  int64
	minUsableSizeForIndexFileCreation int64
}

func newLedgerDirsManager() *ledgerDirsManager {
	return &ledgerDirsManager{writableLedgerDirectories: []string{"./journal"}}
}

func (l *ledgerDirsManager) getWritableLedgerDirs() []string {
	return l.writableLedgerDirectories
}
