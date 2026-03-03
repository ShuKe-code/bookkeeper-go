package bookkeepergo

import (
	"bytes"
	"encoding/binary"
	"os"
	"strconv"
)

const (
	HEADER_SIZE         = 512
	VERSION_HEADER_SIZE = 8
	V1                  = 1
	V5                  = 5
	V6                  = 6
	MB                  = 1024 * 1024
	KB                  = 1024
	cacheDropLagBytes   = 8 * MB
	START_OF_FILE       = -12345
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
	formatVersion        int
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

func newJournalChannel(journalAlignSize int64, preAllocSize int64, fRemoveFromPageCache bool,
	journalDirectory string, logID int64, writeBufferSize, position int64) *journalChannel {
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

	log.Info("Opening journal %s", fileName)
	if PathExists(fileName) { // open an existing file to read.
		fd, err := os.OpenFile(fileName, os.O_RDWR, 0666)
		if err != nil {
			log.Error("Failed to open journal file {}", fileName)
			return nil
		}
		var buf = make([]byte, HEADER_SIZE)
		n, err := fd.Read(buf)
		if err != nil {
			log.Error("Failed to read journal file {}", fileName)
			return nil
		}
		if n == HEADER_SIZE {
			first4 := buf[0:4]
			if bytes.Equal(first4, magicWord) {
				jc.formatVersion = int(binary.BigEndian.Uint32(buf[4:]))
			} else {
				jc.formatVersion = V1
			}
		} else {
			jc.formatVersion = V1
		}
		if position == START_OF_FILE {
			fd.Seek(HEADER_SIZE, 0)
		} else {
			fd.Seek(position, 0)
		}
		jc.fd = fd
	} else { // create new journal file to write, write version
		fd, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			log.Error("Failed to create journal file {}", fileName)
			return nil
		}
		jc.fd = fd
		jc.writeHeader()
	}

	return jc
}

func (jc *journalChannel) writeHeader() {
	buf := make([]byte, HEADER_SIZE)
	copy(buf, magicWord)
	binary.BigEndian.PutUint32(buf[4:], uint32(V6))
	jc.fd.Write(buf)
	jc.bc = NewBufferChannel(jc.writeBufferSize, 0, jc.fd)

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
	return nil
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
