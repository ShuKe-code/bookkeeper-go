package bookkeepergo

type LedgerStorage interface {
	initialize(*Config, *ledgerDirsManager, *ledgerDirsManager)
	start()
	ledgerExists(ledgerId int64) bool
	entryExists(ledgerId int64, entryId int64) bool
	setFenced(ledgerId int64) error
	addEntry(entry []byte) error
	getEntry(ledgerId int64, entryId int64) ([]byte, error)
	getLastAddConfirmed(ledgerId int64) (int64, error)
	flush() error
	checkpoint(Checkpoint) error
	deleteLedger(ledgerId int64)
}

