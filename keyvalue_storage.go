package bookkeepergo

import (
	"path"
	"time"

	"github.com/linxGnu/grocksdb"
)

type KeyValueStorage interface {
	put(key, value []byte) error
	get(key []byte) ([]byte, error)
	gets(key, value []byte) int
	getFloor(key []byte) ([]byte, []byte)
	delete(key []byte) error
	close()
}

type dbConfigType int

const (
	EntryLocation  dbConfigType = 1
	LedgerMetadata dbConfigType = 2
)

type keyValueStorageRocksDB struct {
	db              *grocksdb.DB
	dbPath          string
	optionSync      *grocksdb.WriteOptions
	optionDontSync  *grocksdb.WriteOptions
	optionCache     *grocksdb.ReadOptions
	optionDontCache *grocksdb.ReadOptions
	emptyBatch      *grocksdb.WriteBatch
}

func newKeyValueStorageRocksDB(basePath, subPath string, cfg *Config, dbConfigType dbConfigType) (*keyValueStorageRocksDB, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	if dbConfigType == EntryLocation {
		// ledgerDirsSize := 1
		defaultRocksDBBlockCacheSizeBytes := 10 * 1024 * 1024
		// blockCacheSize := cfg.dbStorageRocksDBBlockSize
		opts.SetWriteBufferSize(uint64(cfg.dbStorageRocksDBWriteBufferSizeMB * MB))
		opts.SetMaxWriteBufferNumber(4)

		if cfg.dbStorageRocksDBNumLevels > 0 {
			opts.SetNumLevels(cfg.dbStorageRocksDBNumLevels)
		}
		opts.SetLevel0FileNumCompactionTrigger(cfg.dbStorageRocksDBNumFilesInLevel0)
		opts.SetMaxBytesForLevelBase(uint64(cfg.dbStorageRocksDBMaxSizeInLevel1MB * MB))
		opts.SetMaxBackgroundJobs(32)
		opts.IncreaseParallelism(32)
		opts.SetMaxTotalWalSize(512 * 1024 * 1024)
		opts.SetMaxOpenFiles(-1)
		opts.SetTargetFileSizeBase(uint64(cfg.dbStorageRocksDBSstSizeInMB * MB))
		opts.SetDeleteObsoleteFilesPeriodMicros(uint64(time.Hour.Microseconds()))

		tableOptions := grocksdb.NewDefaultBlockBasedTableOptions()
		tableOptions.SetBlockSize(defaultRocksDBBlockCacheSizeBytes)
		if cfg.dbStorageRocksDBBloomFilterBitsPerKey > 0 {
			tableOptions.SetFilterPolicy(grocksdb.NewBloomFilter(float64(cfg.dbStorageRocksDBBloomFilterBitsPerKey)))
		}
		tableOptions.SetCacheIndexAndFilterBlocks(true)
		opts.SetLevelCompactionDynamicLevelBytes(true)
		opts.SetBlockBasedTableFactory(tableOptions)

	}
	dbPath := path.Join(basePath, subPath)
	opts.SetKeepLogFileNum(10)

	db, err := grocksdb.OpenDb(opts, dbPath)
	if err != nil {
		return nil, err
	}

	optionSync := grocksdb.NewDefaultWriteOptions()
	optionDontSync := grocksdb.NewDefaultWriteOptions()
	optionCache := grocksdb.NewDefaultReadOptions()
	optionDontCache := grocksdb.NewDefaultReadOptions()
	emptyBatch := grocksdb.NewWriteBatch()

	optionSync.SetSync(true)
	optionDontSync.SetSync(false)

	optionCache.SetFillCache(true)
	optionDontCache.SetFillCache(false)

	return &keyValueStorageRocksDB{
		db:              db,
		dbPath:          dbPath,
		optionSync:      optionSync,
		optionDontSync:  optionDontSync,
		optionCache:     optionCache,
		optionDontCache: optionDontCache,
		emptyBatch:      emptyBatch,
	}, nil
}

func (s *keyValueStorageRocksDB) close() {
	s.db.Close()
}

func (s *keyValueStorageRocksDB) put(key, value []byte) error {
	return s.db.Put(s.optionDontSync, key, value)
}

func (s *keyValueStorageRocksDB) get(key []byte) ([]byte, error) {
	sl, err := s.db.Get(s.optionDontCache, key)
	if err != nil {
		return nil, err
	}
	return sl.Data(), nil
}

func (s *keyValueStorageRocksDB) gets(key, value []byte) int {
	sl, err := s.db.MultiGet(s.optionDontCache, key, value)
	if err != nil {
		return 0
	}
	return len(sl)
}

func (s *keyValueStorageRocksDB) delete(key []byte) error {
	return s.db.Delete(s.optionDontSync, key)
}

func (s *keyValueStorageRocksDB) getFloor(key []byte) ([]byte, []byte) {
	return nil, nil
}