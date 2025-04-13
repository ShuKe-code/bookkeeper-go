package bookkeepergo

import (
	"container/list"
	"encoding/binary"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
)

const (
	LAST_MARK_DEFAULT_NAME = "lastMark"
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
}

func NewJournal(journalDirectory string, cfg *Config) *journal {
	return &journal{
		journalDirectory: journalDirectory,
		config:           cfg,
	}
}

func (j *journal) StartForceWrite(forceWriteChannel chan *ForceWriteRequest) {
	log.Info("ForceWrite Thread started")
	numEntriesInLastForceWrite := 0
	localRequests := make([]*ForceWriteRequest, j.config.journalQueueSize)
	localRequestsIndex := 0
	writeHandlers := list.New()
	for {
		writeHandlers.Init()
		numEntriesInLastForceWrite = 0
		localRequestsIndex = 0
		select {
		case fwr := <-forceWriteChannel:
			localRequests[localRequestsIndex] = fwr
			localRequestsIndex++
		}
		// dequeue force write requests
	dequeLoop:
		for {
			select {
			case fwr := <-forceWriteChannel:
				localRequests[localRequestsIndex] = fwr
				localRequestsIndex++
			default:
				break dequeLoop
			}
		}
		// Sync and mark the journal up to the position of the last entry in the batch
		lastRequest := localRequests[localRequestsIndex-1]
		forceWriteQueueSize.Add(float64(-localRequestsIndex))
		j.syncJournal(lastRequest)

		for i := 0; i < localRequestsIndex; i++ {
			req := localRequests[i]
			numEntriesInLastForceWrite += req.process(*writeHandlers)
			req.release()
		}
		forceWriteQueueSize.Add(float64(numEntriesInLastForceWrite))
		forceWriteGroupingCountStats.WithLabelValues("true").Observe(float64(numEntriesInLastForceWrite))
	}
}

func (j *journal) syncJournal(lastRequest *ForceWriteRequest) {
	fsyncStartTime := time.Now().UnixNano()
	lastRequest.flushFileToDisk()
	fsyncStartTime = time.Now().UnixNano() - fsyncStartTime
	journalSyncStats.WithLabelValues("true").Observe(float64(fsyncStartTime))
}

func (j *journal) startJournal(entry *queueEntry) {
	log.Info("Starting journal on ", j.journalDirectory)
	toFlush := list.New()
	numEntriesToFlush := 0
	lenBuff := make([]byte, 4)
	var bc BufferChannel
	var logFile *journalChannel
	forceWriteQueue := make(chan *ForceWriteRequest, j.config.journalQueueSize)
	queue := make(chan *queueEntry, j.config.journalQueueSize)
	go j.StartForceWrite(forceWriteQueue)
	batchSize := 0

	journalIds, err := ListJournalIds(j.journalDirectory, nil)
	if err != nil {
		log.Error("Failed to list journal ids")
		return
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
	for {
		if logFile == nil {
			logId = logId + 1
			journalCreationWatcher = time.Now().UnixNano()
			logFile = NewJournalChannel(j.journalAlignmentSize, j.journalPreAllocSize, j.removePagesFromCache,
				j.journalDirectory, logId, j.journalWriteBufferSize)
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
				select {
				case qe = <-queue:
					localQueueEntries[localQueueEntriesIdx] = qe
					localQueueEntriesIdx++
					localQueueEntriesLen++
				}
			loop1:
				for {
					select {
					case qe = <-queue:
						localQueueEntries[localQueueEntriesIdx] = qe
						localQueueEntriesIdx++
						localQueueEntriesLen++
					default:
						break loop1
					}
				}
			} else {
				pollWaitTimeNanos := j.maxGroupWaitInNanos - toFlush.Front().Value.(*queueEntry).enqueueTime
				if pollWaitTimeNanos < 0 {
					pollWaitTimeNanos = 0
				}
			loop2:
				for {
					select {
					case qe = <-queue:
						localQueueEntries[localQueueEntriesIdx] = qe
						localQueueEntriesIdx++
						localQueueEntriesLen++
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
				journalFlushWatcher = time.Now().UnixNano()
				bc.flush()
				for item := toFlush.Front(); item != nil; item = item.Next() {
					entry := item.Value.(*queueEntry)
					if entry != nil && (!j.syncData || entry.ackBeforeSync) {
						item.Value = nil
						numEntriesToFlush--
					}
					entry.run()
				}
				writeHandlers.Init()
				lastFlushPosition = bc.position()
				journalFlushStats.WithLabelValues("true").Observe(float64(time.Now().UnixNano() - journalFlushWatcher))
				forceWriteBatchBytesStats.WithLabelValues("true").Observe(float64(batchSize))
				forceWriteBatchEntriesStats.WithLabelValues("true").Observe(float64(numEntriesToFlush))
				shouldRolloverJournal := lastFlushPosition > j.maxJournalSize
				if j.syncData || shouldRolloverJournal || time.Now().UnixNano()-lastFlushTimeMs > j.journalPageCacheFlushIntervalMSec {
					forceWriteQueue <- NewForceWriteRequest(logFile, logId, shouldRolloverJournal, uint64(lastFlushPosition), toFlush)
				}
				toFlush.Init()
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
		if qe != nil {
			continue
		}
		journalQueueSize.Desc()
		journalQueueStats.WithLabelValues("true").Observe(float64(time.Now().UnixNano() - qe.enqueueTime))

		entrySize := len(qe.entry)
		journalWriteBytes.Add(float64(entrySize))

		batchSize += (4 + entrySize)
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

type JournalIdFilter interface {
	Accept(id int64) bool
}

// ListJournalIds lists journal IDs in the specified directory, applying the optional filter.
func ListJournalIds(journalDir string, filter JournalIdFilter) ([]int64, error) {
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
