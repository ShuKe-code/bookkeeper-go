package bookkeepergo

import (
	"encoding/binary"
	"sync/atomic"
)

type logMark struct {
	logFileId     atomic.Uint64
	logFileOffset atomic.Uint64
}

func (lm *logMark) getLogFileId() uint64 {
	return lm.logFileId.Load()
}

func (lm *logMark) getLogFileOffset() uint64 {
	return lm.logFileOffset.Load()
}

func (lm *logMark) readLogMark(buf []byte) {
	lm.logFileId.Store(binary.BigEndian.Uint64(buf[0:]))
	lm.logFileOffset.Store(binary.BigEndian.Uint64(buf[8:]))
}

func (lm *logMark) writeLogMark(buf []byte) {
	binary.BigEndian.PutUint64(buf[0:], lm.logFileId.Load())
	binary.BigEndian.PutUint64(buf[8:], lm.logFileOffset.Load())
}

func (lm *logMark) setLogMark(logFileId uint64, logFileOffset uint64) {
	lm.logFileId.Store(logFileId)
	lm.logFileOffset.Store(logFileOffset)
}

func (lm *logMark) compare(newLm *logMark) int {
	ret := int64(lm.logFileId.Load()) - int64(newLm.logFileId.Load())
	if ret == 0 {
		ret = int64(lm.logFileOffset.Load()) - int64(newLm.logFileOffset.Load())
	}
	if ret > 0 {
		return 1
	} else if ret < 0 {
		return -1
	}
	return 0
}
