package bookkeepergo

import (
	"bytes"
	"os"
	"strconv"

	"github.com/pingcap/log"
)

const (
	HEADER_SIZE       = 512
	V5                = 5
	MB                = 1024 * 1024
	cacheDropLagBytes = 8 * MB
)

var (
	magicWord = []byte("BKLG")
)

type journalChannel struct {
	bc                   BufferChannel
	journalAlignSize     int64
	fRemoveFromPageCache bool
	journalDirectory     string
	logID                int64
	writeBufferSize      int64
	preAllocSize         int64
	nextPrealloc         int64
	lastDropPosition     int64
	fd                   *os.File
	zeros                []byte
}

func PathExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}

func NewJournalChannel(journalAlignSize int64, preAllocSize int64, fRemoveFromPageCache bool,
	journalDirectory string, logID int64, writeBufferSize int64) *journalChannel {
	jc := &journalChannel{
		journalAlignSize:     journalAlignSize,
		fRemoveFromPageCache: fRemoveFromPageCache,
		journalDirectory:     journalDirectory,
		logID:                logID,
		writeBufferSize:      writeBufferSize,
	}
	jc.preAllocSize = preAllocSize - preAllocSize%journalAlignSize
	jc.zeros = make([]byte, jc.journalAlignSize)
	fileName := journalDirectory + strconv.FormatInt(logID, 16) + ".txn"

	log.Info("Opening journal {}", journalDirectory)
	if PathExists(fileName) { // open an existing file to read.

	} else { // create new journal file to write, write version
		fd, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			log.Error("Failed to open journal file {}", fileName)
			return nil
		}
		jc.fd = fd
		jc.writeHeader()
		jc.bc = NewBufferChannel(jc.writeBufferSize)
	}

	return jc
}

func (jc *journalChannel) writeHeader() {
	byteBuf := bytes.NewBuffer(make([]byte, HEADER_SIZE))
	byteBuf.Write(magicWord)
	byteBuf.Write([]byte{V5, 0, 0, 0})
	jc.fd.Write(byteBuf.Bytes())
	jc.forceWrite(true)
	jc.nextPrealloc = jc.preAllocSize
	jc.fd.WriteAt(jc.zeros, jc.nextPrealloc-jc.journalAlignSize)
}

func (jc *journalChannel) preAllocIfNeeded(size int64) {
	if jc.bc.position() >= jc.nextPrealloc {
		jc.nextPrealloc += jc.preAllocSize
		jc.fd.WriteAt(jc.zeros, jc.nextPrealloc-jc.journalAlignSize)
	}
}

func (jc *journalChannel) forceWrite(forceMetadata bool) error {
	log.Debug("Journal ForceWrite")
	newForceWritePosition, err := jc.bc.forceWrite(forceMetadata)
	if err != nil {
		return err
	}
	if jc.fRemoveFromPageCache {
		newDropPos := newForceWritePosition - cacheDropLagBytes
		if newDropPos > jc.lastDropPosition {
			bestEffortRemoveFromPageCache(jc.fd, jc.lastDropPosition, newDropPos-jc.lastDropPosition)
		}
		jc.lastDropPosition = newDropPos
	}
}

func bestEffortRemoveFromPageCache(file *os.File, offset int64, length int64) error {
	// fd := int(file.Fd())
	// err := unix.Fadvise(fd, offset, length, unix.FADV_DONTNEED)
	// if err != nil && err != unix.ENOSYS {
	// 	return err
	// }
	return nil
}

func (jc *journalChannel) getBufferedChannel() BufferChannel {
	return jc.bc
}

func (jc *journalChannel) close() {
	if jc.bc != nil {
		jc.bc.close()
	} else {
		jc.fd.Close()
	}
}
