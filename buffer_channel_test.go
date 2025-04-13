package bookkeepergo

import (
	"os"
	"testing"

	"github.com/c2fo/testify/assert"
)

func TestBufferChannelImpl(t *testing.T) {
	fd, _ := os.OpenFile("./journal/test.txn", os.O_RDWR|os.O_CREATE, 0666)

	bc := NewBufferChannel(10, 0, fd)
	bc.write([]byte("test"))
	assert.Equal(t, int64(4), bc.position())

	bc.write([]byte("test"))
	assert.Equal(t, int64(8), bc.position())

	bc.flush()
	bc.close()
}
