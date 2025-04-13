package bookkeepergo

type Config struct {
	journalQueueSize                  int64
	journalMaxMemorySizeMb            int64
	journalPageCacheFlushIntervalMSec int64
}
