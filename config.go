package bookkeepergo

type Config struct {
	enableBusyWait bool
	// Journal
	journalQueueSize                  int64
	journalMaxMemorySizeMb            int64
	journalPageCacheFlushIntervalMSec int64
	journalMaxSizeMB                  int64
	journalMaxBackups                 int64
	journalSyncData                   bool
	journalWriteData                  bool
	journalAdaptiveGroupWrites        bool
	journalMaxGroupWaitMSec           int64
	journalBufferedWritesThreshold    int64
	journalBufferedEntriesThreshold   int64
	journalFlushWhenQueueEmpty        bool
	journalRemoveFromPageCache        bool
	journalPreAllocSizeMB             int64
	journalWriteBufferSizeKB          int64
	journalAlignmentSize              int64

	// Ledger Storage (RocksDB)
	dbStorageRocksDBWriteBufferSizeMB     int64
	dbStorageRocksDBSstSizeInMB           int
	dbStorageRocksDBBlockSize             int
	dbStorageRocksDBBloomFilterBitsPerKey int
	dbStorageRocksDBNumLevels             int
	dbStorageRocksDBNumFilesInLevel0      int
	dbStorageRocksDBMaxSizeInLevel1MB     int64
}

func fixConfig(config *Config) {
	if config.journalQueueSize == 0 {
		config.journalQueueSize = 1000
	}
	if config.journalMaxMemorySizeMb == 0 {
		config.journalMaxMemorySizeMb = 100
	}
	if config.journalPageCacheFlushIntervalMSec == 0 {
		config.journalPageCacheFlushIntervalMSec = 1000
	}
	if config.journalMaxSizeMB == 0 {
		config.journalMaxSizeMB = 2 * 1024
	}
	if config.journalMaxBackups == 0 {
		config.journalMaxBackups = 5
	}
	if config.journalMaxGroupWaitMSec == 0 {
		config.journalMaxGroupWaitMSec = 2
	}
	if config.journalBufferedWritesThreshold == 0 {
		config.journalBufferedWritesThreshold = 512 * 1024
	}
	if config.journalBufferedEntriesThreshold == 0 {
		config.journalBufferedEntriesThreshold = 0
	}
	if config.journalPreAllocSizeMB == 0 {
		config.journalPreAllocSizeMB = 16
	}
	if config.journalWriteBufferSizeKB == 0 {
		config.journalWriteBufferSizeKB = 64
	}
	if config.journalAlignmentSize == 0 {
		config.journalAlignmentSize = 512
	}

	if config.dbStorageRocksDBWriteBufferSizeMB == 0 {
		config.dbStorageRocksDBWriteBufferSizeMB = 64
	}
	if config.dbStorageRocksDBSstSizeInMB == 0 {
		config.dbStorageRocksDBSstSizeInMB = 64
	}
	if config.dbStorageRocksDBBlockSize == 0 {
		config.dbStorageRocksDBBlockSize = 65536
	}
	if config.dbStorageRocksDBBloomFilterBitsPerKey == 0 {
		config.dbStorageRocksDBBloomFilterBitsPerKey = 10
	}
	if config.dbStorageRocksDBNumLevels == 0 {
		config.dbStorageRocksDBNumLevels = -1
	}
	if config.dbStorageRocksDBNumFilesInLevel0 == 0 {
		config.dbStorageRocksDBNumFilesInLevel0 = 4
	}
	if config.dbStorageRocksDBMaxSizeInLevel1MB == 0 {
		config.dbStorageRocksDBMaxSizeInLevel1MB = 256
	}
}

func (c *Config) getJournalDirs() []string {
	return []string{}
}
