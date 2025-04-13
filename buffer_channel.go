package bookkeepergo

import (
	"bufio"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

type BufferChannel interface {
	write([]byte) error
	flush() error
	forceWrite(bool) (int64, error)
	read([]byte, int64, int) error
	flushAndForceWrite(bool) error
	position() int64
	close()
}

type BufferChannelImpl struct {
	writeCapacity            int64
	writeBufferStartPosition atomic.Int64
	writeBuffer              bufio.Writer
	nowPosition              int64
	unpersistedBytesBound    int64
	doRegularFlushes         bool
	unpersistedBytes         atomic.Int64
	fd                       *os.File
	closed                   bool
	mu                       sync.Mutex
}

func NewBufferChannel(writeBufferSize, unpersistedBytesBound int64, fd *os.File) BufferChannel {
	wl := bufio.NewWriterSize(fd, int(writeBufferSize))
	doRegularFlushes := false
	if unpersistedBytesBound > 0 {
		doRegularFlushes = true
	}
	position, _ := fd.Seek(0, io.SeekCurrent)
	bc := &BufferChannelImpl{
		writeCapacity:         writeBufferSize,
		writeBuffer:           *wl,
		nowPosition:           position,
		unpersistedBytesBound: unpersistedBytesBound,
		doRegularFlushes:      doRegularFlushes,
		fd:                    fd,
	}
	bc.writeBufferStartPosition.Store(position)
	bc.unpersistedBytes.Store(0)
	return bc
}

func (bc *BufferChannelImpl) write(src []byte) error {
	shouldForceWrite := false
	bc.mu.Lock()
	bc.writeBuffer.Write(src)
	bc.nowPosition += int64(len(src))

	if bc.doRegularFlushes {
		bc.unpersistedBytes.Add(int64(len(src)))
		if bc.unpersistedBytes.Load() >= bc.unpersistedBytesBound {
			bc.flush()
			shouldForceWrite = true
		}
	}
	bc.mu.Unlock()

	if shouldForceWrite {
		_, err := bc.forceWrite(false)
		if err != nil {
			return err
		}
	}
	return nil
}

func (bc *BufferChannelImpl) flush() error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	bc.writeBuffer.Flush()
	position, _ := bc.fd.Seek(0, io.SeekCurrent)
	bc.writeBufferStartPosition.Store(position)
	return nil
}

func (bc *BufferChannelImpl) forceWrite(bool) (int64, error) {
	positionForceWrite := bc.writeBufferStartPosition.Load()
	if bc.unpersistedBytesBound > 0 {
		bc.mu.Lock()
		defer bc.mu.Unlock()
		bc.unpersistedBytes.Store(int64(bc.writeBuffer.Buffered()))
	}
	if err := bc.fd.Sync(); err != nil {
		return positionForceWrite, err
	}
	return positionForceWrite, nil
}

func (bc *BufferChannelImpl) read([]byte, int64, int) error {
	return nil
}

func (bc *BufferChannelImpl) flushAndForceWrite(bool) error {
	bc.flush()
	bc.forceWrite(false)
	return nil
}

func (bc *BufferChannelImpl) position() int64 {
	return bc.nowPosition
}

func (bc *BufferChannelImpl) close() {
	if bc.closed {
		return
	}
	bc.writeBuffer.Reset(nil)
	bc.fd.Close()
	bc.closed = true
}
