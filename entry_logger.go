package bookkeepergo

type EntryLogger interface {
	addEntry(ledgerId int64, entry []byte) error
	readEntry(ledgerId,entryId int64, entryLocation int64) ([]byte, error)
	flush() error
	close()
	scanEntryLog(ledgerId int64, scanner EntryLogScanner)
	logExists(logId int64) bool
	removeEntryLog(entryLogId int64)
}