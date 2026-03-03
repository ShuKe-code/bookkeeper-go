package bookkeepergo

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	LAST_MARK_DEFAULT_NAME        = "lastMark"
	PADDING_MASK           uint32 = 0xFFFFFF00
)

type journal struct {
	journalDirectory                  string
	queue                             chan *queueEntry
	config                            *Config
	maxJournalSize                    int64
	journalPreAllocSize               int64
	journalWriteBufferSize            int64
	syncData                          bool
	maxBackupJournals                 int
	maxGroupWaitInNanos               int64
	bufferedWritesThreshold           int64
	bufferedEntriesThreshold          int64
	journalAlignmentSize              int64
	journalPageCacheFlushIntervalMSec int64
	removePagesFromCache              bool
	lastMarkFileName                  string
	flushWhenQueueEmpty               bool
	running                           bool
	forceWriteQueueSize               int64
	lastLogMark                       *lastLogMark
}

func NewJournal(journalIndex int, journalDirectory string, cfg *Config,
	ledgerDirsManager *ledgerDirsManager) *journal {
	fixConfig(cfg)

	var lastMarkFileName string
	if len(cfg.getJournalDirs()) == 1 {
		lastMarkFileName = LAST_MARK_DEFAULT_NAME
	} else {
		lastMarkFileName = fmt.Sprintf("%s.%d", LAST_MARK_DEFAULT_NAME, journalIndex)
	}

	lastLogMark := newLastLogMark(0, 0, ledgerDirsManager, lastMarkFileName)
	lastLogMark.readLog()
	log.Debug("Last Log Mark : %v", lastLogMark.getCurMark())
	return &journal{
		journalDirectory:                  journalDirectory,
		config:                            cfg,
		maxJournalSize:                    cfg.journalMaxSizeMB * MB,
		journalPreAllocSize:               cfg.journalPreAllocSizeMB * MB,
		journalWriteBufferSize:            cfg.journalWriteBufferSizeKB * KB,
		syncData:                          cfg.journalSyncData,
		maxBackupJournals:                 int(cfg.journalMaxBackups),
		maxGroupWaitInNanos:               cfg.journalMaxGroupWaitMSec * int64(time.Millisecond),
		bufferedWritesThreshold:           cfg.journalBufferedWritesThreshold,
		bufferedEntriesThreshold:          cfg.journalBufferedEntriesThreshold,
		journalAlignmentSize:              cfg.journalAlignmentSize,
		journalPageCacheFlushIntervalMSec: cfg.journalPageCacheFlushIntervalMSec,
		removePagesFromCache:              cfg.journalRemoveFromPageCache,
		lastMarkFileName:                  LAST_MARK_DEFAULT_NAME,
		flushWhenQueueEmpty:               cfg.journalFlushWhenQueueEmpty,
		queue:                             make(chan *queueEntry, cfg.journalQueueSize),
		lastLogMark:                       lastLogMark,
	}
}

func (j *journal) logAddEntry(ledgerId, entryId uint64, entry []byte, ackBeforeSync bool, cb WriteCallback) {
	journalQueueSize.Inc()
	j.queue <- NewQueueEntry(ledgerId, entryId, entry, time.Now().UnixNano(), ackBeforeSync, cb)
}

func (j *journal) startForceWrite(forceWriteChannel chan *ForceWriteRequest) {
	log.Info("ForceWrite Thread started")
	initForceWriteQueueSize(func() float64 { return float64(j.forceWriteQueueSize) })
	numEntriesInLastForceWrite := 0
	localRequests := make([]*ForceWriteRequest, j.config.journalQueueSize)
	var localRequestsIndex int64
	var writeHandlers *list.List
	if j.config.enableBusyWait {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}
	for {
		writeHandlers = list.New()
		numEntriesInLastForceWrite = 0
		localRequestsIndex = 0
		fwr := <-forceWriteChannel
		localRequests[localRequestsIndex] = fwr
		localRequestsIndex++
		// dequeue force write requests
	dequeLoop:
		for {
			select {
			case fwr := <-forceWriteChannel:
				localRequests[localRequestsIndex] = fwr
				localRequestsIndex++
				if localRequestsIndex == j.config.journalQueueSize {
					break dequeLoop
				}
			default:
				break dequeLoop
			}
		}
		// Sync and mark the journal up to the position of the last entry in the batch
		lastRequest := localRequests[localRequestsIndex-1]
		j.forceWriteQueueSize -= localRequestsIndex
		j.syncJournal(lastRequest)
		var i int64
		for i = 0; i < localRequestsIndex; i++ {
			req := localRequests[i]
			numEntriesInLastForceWrite += req.process(*writeHandlers)
			req.release()
		}
		forceWriteGroupingCountStats.WithLabelValues("true").Observe(float64(numEntriesInLastForceWrite))
	}
}

func (j *journal) syncJournal(lastRequest *ForceWriteRequest) {
	fsyncStartTime := time.Now().UnixNano()
	lastRequest.flushFileToDisk()
	fsyncStartTime = time.Now().UnixNano() - fsyncStartTime
	journalSyncStats.WithLabelValues("true").Observe(float64(fsyncStartTime))
	j.lastLogMark.setCurLogMark(lastRequest.logId, int64(lastRequest.lastFlushedPosition))
}


func (j *journal) scanJournal(journalId, journalPos int64, scanner JournalScanner, skipInvalidRecord bool) int64 {
	recLog := newJournalChannel(j.journalAlignmentSize, j.journalPreAllocSize, j.removePagesFromCache,
		j.journalDirectory, journalId, j.journalWriteBufferSize, journalPos)

	lenBuff := make([]byte, 4)
	recBuff := make([]byte, 64*1024)
	for {
		offset, _ := recLog.fd.Seek(0, io.SeekCurrent)
		clearLenbuf(lenBuff)
		if _, err := recLog.fd.Read(lenBuff); err != nil {
			break
		}
		length := int(binary.BigEndian.Uint32(lenBuff))
		if length == 0 {
			break
		}
		isPaddingRecord := false
		if uint32(length) == PADDING_MASK {
			// skip padding bytes
			isPaddingRecord = true
			clearLenbuf(lenBuff)
			if _, err := recLog.fd.Read(lenBuff); err != nil {
				break
			}
			length = int(binary.BigEndian.Uint32(lenBuff))
			if length == 0 {
				continue
			}
		}
		clearLenbuf(recBuff)
		if len(recBuff) < length {
			recBuff = make([]byte, length)
		}
		recLog.fd.Read(recBuff[:length])
		if !isPaddingRecord {
			scanner.process(0, offset, recBuff[:length])
		}
	}
	offset, _ := recLog.fd.Seek(0, io.SeekCurrent)
	return  offset
}

func (j *journal) startJournal() {
	log.Info("Starting journal on %s", j.journalDirectory)
	j.running = true
	toFlush := list.New()
	numEntriesToFlush := 0
	var lenBuff = make([]byte, 4)

	var paddingBuffer = make([]byte, j.config.journalAlignmentSize*2)
	binary.BigEndian.PutUint32(paddingBuffer, uint32(PADDING_MASK))

	var bc BufferChannel
	var logFile *journalChannel
	forceWriteQueue := make(chan *ForceWriteRequest, j.config.journalQueueSize)
	queue := j.queue
	go j.startForceWrite(forceWriteQueue)
	batchSize := 0

	journalIds, err := ListJournalIds(j.journalDirectory, nil)
	if err != nil {
		log.Error("Failed to list journal ids")
	}
	logId := time.Now().UnixMilli()
	if len(journalIds) > 0 {
		logId = journalIds[len(journalIds)-1]
	}
	var lastFlushPosition int64
	var journalCreationWatcher int64
	var journalFlushWatcher int64
	groupWhenTimeout := false
	var dequeueStartTime int64
	lastFlushTimeMs := time.Now().UnixMilli()
	writeHandlers := list.New()
	localQueueEntries := make([]*queueEntry, j.config.journalQueueSize)
	localQueueEntriesIdx := 0
	localQueueEntriesLen := 0
	var qe *queueEntry

	if j.config.enableBusyWait {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}
	for {
		if logFile == nil {
			logId = logId + 1
			journalCreationWatcher = time.Now().UnixNano()
			logFile = newJournalChannel(j.journalAlignmentSize, j.journalPreAllocSize, j.removePagesFromCache,
				j.journalDirectory, logId, j.journalWriteBufferSize, 0)
			journalCreationStats.WithLabelValues("true").Observe(float64(time.Now().UnixNano() - journalCreationWatcher))
			bc = logFile.getBufferedChannel()
			lastFlushPosition = bc.position()
		}
		if qe == nil {
			if dequeueStartTime != 0 {
				journalProcessTimeStats.WithLabelValues("true").Observe(float64(time.Now().UnixNano() - dequeueStartTime))
			}
			localQueueEntriesIdx = 0
			localQueueEntriesLen = 0
			if numEntriesToFlush == 0 {
				qe = <-queue
				localQueueEntries[localQueueEntriesLen] = qe
				localQueueEntriesLen++
			loop1:
				for {
					select {
					case qe = <-queue:
						localQueueEntries[localQueueEntriesLen] = qe
						localQueueEntriesLen++
						if localQueueEntriesLen == len(localQueueEntries) {
							break loop1
						}
					default:
						break loop1
					}
				}
			} else {
				pollWaitTimeNanos := j.maxGroupWaitInNanos - (time.Now().UnixNano() - toFlush.Front().Value.(*queueEntry).enqueueTime)
				if pollWaitTimeNanos < 0 {
					pollWaitTimeNanos = 0
				}
			loop2:
				for {
					select {
					case qe = <-queue:
						localQueueEntries[localQueueEntriesLen] = qe
						localQueueEntriesLen++
						if localQueueEntriesLen == len(localQueueEntries) {
							break loop2
						}
					case <-time.After(time.Duration(pollWaitTimeNanos) * time.Nanosecond):
						break loop2
					}
				}
			}
			dequeueStartTime = time.Now().UnixNano()
			if localQueueEntriesLen > 0 {
				qe = localQueueEntries[localQueueEntriesIdx]
				localQueueEntries[localQueueEntriesIdx] = nil
				localQueueEntriesIdx++
			}
		}
		if numEntriesToFlush > 0 {
			shouldFlush := false
			// We should issue a forceWrite if any of the three conditions below holds good
			// 1. If the oldest pending entry has been pending for longer than the max wait time
			if j.maxGroupWaitInNanos > 0 && !groupWhenTimeout &&
				time.Now().UnixNano()-toFlush.Front().Value.(*queueEntry).enqueueTime > j.maxGroupWaitInNanos {
				shouldFlush = true
			} else if j.maxGroupWaitInNanos > 0 && groupWhenTimeout &&
				(qe == nil || time.Now().UnixNano()-qe.enqueueTime > j.maxGroupWaitInNanos) {
				// when group timeout, it would be better to look forward, as there might be lots of
				// entries already timeout
				// due to a previous slow write (writing to filesystem which impacted by force write).
				// Group those entries in the queue
				// a) already timeout
				// b) limit the number of entries to group
				groupWhenTimeout = false
				shouldFlush = true
				flushMaxWaitCounter.Inc()
			} else if qe != nil &&
				((j.bufferedEntriesThreshold > 0 && int64(toFlush.Len()) >= j.bufferedEntriesThreshold) ||
					(j.bufferedWritesThreshold > 0 && bc.position() >= j.bufferedWritesThreshold+lastFlushPosition)) {
				// 2. If we have buffered more than the buffWriteThreshold or bufferedEntriesThreshold
				shouldFlush = true
				groupWhenTimeout = false
				flushMaxOutstandingBytesCounter.Inc()
			} else if qe == nil && j.flushWhenQueueEmpty {
				shouldFlush = true
				groupWhenTimeout = false
				flushEmptyQueueCounter.Inc()
			}
			if shouldFlush {
				j.writePaddingBytes(logFile, j.journalAlignmentSize, paddingBuffer)
				journalFlushWatcher = time.Now().UnixNano()
				bc.flush()
				for item := toFlush.Front(); item != nil; item = item.Next() {
					entry := item.Value.(*queueEntry)
					if entry != nil && (!j.syncData || entry.ackBeforeSync) {
						numEntriesToFlush--
						entry.run()
						item.Value = nil
					}
				}
				writeHandlers.Init()
				lastFlushPosition = bc.position()
				journalFlushStats.WithLabelValues("true").Observe(float64(time.Now().UnixNano() - journalFlushWatcher))
				forceWriteBatchBytesStats.WithLabelValues("true").Observe(float64(batchSize))
				forceWriteBatchEntriesStats.WithLabelValues("true").Observe(float64(numEntriesToFlush))
				shouldRolloverJournal := lastFlushPosition > j.maxJournalSize
				if shouldRolloverJournal {
					fmt.Println("shouldRolloverJournal:", shouldRolloverJournal, "lastFlushPosition:", lastFlushPosition, "maxJournalSize:", j.maxJournalSize)
				}
				if j.syncData || shouldRolloverJournal || time.Now().UnixNano()-lastFlushTimeMs > j.journalPageCacheFlushIntervalMSec {
					forceWriteQueue <- NewForceWriteRequest(logFile, logId, shouldRolloverJournal, uint64(lastFlushPosition), toFlush)
					j.forceWriteQueueSize++
				}
				toFlush = list.New()
				numEntriesToFlush = 0
				batchSize = 0
				if shouldRolloverJournal {
					logFile = nil
					continue
				}
			}
		}
		if !j.running {
			log.Info("Journal Manager is asked to shut down, quit.")
			break
		}
		if qe == nil {
			continue
		}
		journalQueueSize.Desc()
		journalQueueStats.WithLabelValues("true").Observe(float64(time.Now().UnixNano() - qe.enqueueTime))

		entrySize := len(qe.entry)
		journalWriteBytes.Add(float64(entrySize))

		batchSize += (4 + entrySize)
		clearLenbuf(lenBuff)
		binary.BigEndian.PutUint32(lenBuff, uint32(entrySize))

		logFile.preAllocIfNeeded(int64(entrySize + 4))
		bc.write(lenBuff)
		bc.write(qe.entry)
		qe.entry = nil
		toFlush.PushBack(qe)
		numEntriesToFlush++
		if localQueueEntriesIdx < localQueueEntriesLen {
			qe = localQueueEntries[localQueueEntriesIdx]
			localQueueEntries[localQueueEntriesIdx] = nil
			localQueueEntriesIdx++
		} else {
			qe = nil
		}
	}
}

func (j *journal) writePaddingBytes(jc *journalChannel, journalAlignSize int64, paddingBuffer []byte) {
	bytesToAlign := jc.bc.position() % journalAlignSize
	if bytesToAlign != 0 {
		paddingBytes := journalAlignSize - bytesToAlign
		if paddingBytes < 8 {
			paddingBytes = journalAlignSize - (8 - paddingBytes)
		} else {
			paddingBytes -= 8
		}
		clearLenbuf(paddingBuffer[4:8])
		binary.BigEndian.PutUint32(paddingBuffer[4:], uint32(paddingBytes))
		len := 8 + paddingBytes
		jc.preAllocIfNeeded(len)
		jc.bc.write(paddingBuffer[:len])
	}
}

func (j *journal) getLastLogMark() *lastLogMark {
	return j.lastLogMark
}

func (j *journal) setLastLogMark(id, offset int64) {
	j.lastLogMark.setCurLogMark(id, offset)
}

func (j *journal) newCheckpoint() Checkpoint {
	return &logMarkCheckpoint{
		mark: j.lastLogMark.markLog(),
	}
}

func (j *journal) checkpointComplete(checkpoint Checkpoint, compact bool) error {
	lmcheckpoint, ok := checkpoint.(*logMarkCheckpoint)
	if !ok {
		return nil
	}
	mark := lmcheckpoint.mark
	mark.rollLog()
	if compact {
		logs, err := ListJournalIds(j.journalDirectory, &journalRollingFilter{mark})
		if err != nil {
			return err
		}
		if len(logs) > j.maxBackupJournals {
			maxIdx := len(logs) - j.maxBackupJournals
			for i := 0; i < maxIdx; i++ {
				id := logs[i]
				if id < int64(j.lastLogMark.getCurMark().getLogFileId()) {
					fileName := path.Join(j.journalDirectory, fmt.Sprintf("%016x.txn", id))
					if err := os.Remove(fileName); err != nil {
						log.Warn("Could not delete old journal file %s", fileName)
					} else {
						log.Info("garbage collected journal %s", fileName)
					}
				}
			}
		}
	}
	return nil

}

type JournalIdFilter interface {
	Accept(id int64) bool
}

type journalRollingFilter struct {
	lastMark *lastLogMark
}

func (jrf *journalRollingFilter) Accept(id int64) bool {
	return id < int64(jrf.lastMark.getCurMark().getLogFileId())
}

// ListJournalIds lists journal IDs in the specified directory, applying the optional filter.
func ListJournalIds(journalDir string, filter JournalIdFilter) ([]int64, error) {
	if err := createDirectory(journalDir); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(journalDir)
	if err != nil {
		return nil, err
	}

	var ids []int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".txn") {
			continue
		}
		idStr := strings.SplitN(name, ".", 2)[0]
		id, err := strconv.ParseInt(idStr, 16, 64)
		if err != nil {
			continue // skip invalid file name
		}
		if filter != nil {
			if filter.Accept(id) {
				ids = append(ids, id)
			}
		} else {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	return ids, nil
}

func createDirectory(dirPath string) error {
	f, e := os.Stat(dirPath)
	if e != nil {
		if os.IsNotExist(e) {
			return os.MkdirAll(dirPath, 0755)
		}
		return fmt.Errorf("failed to create dir %s: %v", dirPath, e)
	}
	if !f.IsDir() {
		return fmt.Errorf("failed to create dir %s: dir path already exists and is not a directory", dirPath)
	}
	return e
}

func clearLenbuf(lenBuff []byte) {
	for i := 0; i < len(lenBuff); i++ {
		lenBuff[i] = 0
	}
}
