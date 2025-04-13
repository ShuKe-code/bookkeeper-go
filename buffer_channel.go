package bookkeepergo

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
}

func NewBufferChannel(writeBufferSize int64) BufferChannel {
	return &BufferChannelImpl{}
}

func (bc *BufferChannelImpl) write([]byte) error {
	return nil
}

func (bc *BufferChannelImpl) flush() error {
	return nil
}

func (bc *BufferChannelImpl) forceWrite(bool) (int64, error) {
	return 0, nil
}

func (bc *BufferChannelImpl) read([]byte, int64, int) error {
	return nil
}

func (bc *BufferChannelImpl) flushAndForceWrite(bool) error {
	return nil
}

func (bc *BufferChannelImpl) position() int64 {
	return 0
}

func (bc *BufferChannelImpl) close() {}
