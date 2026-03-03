package bookkeepergo

import (
	"fmt"
	"testing"

	"github.com/c2fo/testify/assert"
)

func TestWriteCache(t *testing.T) {
	cache, err := newWriteCache(10840, 1024)
	assert.NoError(t, err)
	entry, ok := cache.get(10, 20)
	assert.False(t, ok)
	assert.Nil(t, entry)

	cache.put(10, 20, []byte{1, 2, 3})
	ok = cache.hasEntry(10, 20)
	assert.True(t, ok)
	entry, ok = cache.get(10, 20)
	assert.True(t, ok)
	assert.Equal(t, []byte{1, 2, 3}, entry)
	entry, ok = cache.getLastEntry(10)
	assert.True(t, ok)
	assert.Equal(t, []byte{1, 2, 3}, entry)

	cache.forEach(func(ledgerId, entryId int64, entry []byte) {
		fmt.Println("ledgerId", ledgerId, "entryId", entryId, "entry", string(entry))
	})
}
