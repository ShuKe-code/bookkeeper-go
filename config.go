package bookkeepergo

type Config struct {
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
}
