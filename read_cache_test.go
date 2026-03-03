package bookkeepergo

import (
	"testing"

	"github.com/c2fo/testify/assert"
)

func TestReadCache(t *testing.T) {
	cache, err := newReadCache(256, 64)
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
	cnt := cache.count()
	assert.Equal(t, 1, cnt)

	cache.put(10, 21, []byte{1, 2, 3})
	ok = cache.hasEntry(10, 21)
	assert.True(t, ok)
	cache.put(10, 22, []byte{1, 2, 3})
	cache.put(10, 23, []byte{1, 2, 3})

	ok = cache.hasEntry(10, 22)
	assert.True(t, ok)
	ok = cache.hasEntry(10, 23)
	assert.True(t, ok)

	cache.put(10, 24, []byte{1, 2, 3})
	ok = cache.hasEntry(10, 20)
	assert.False(t, ok)
}
