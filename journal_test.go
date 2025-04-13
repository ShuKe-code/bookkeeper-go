package bookkeepergo

import (
	"sync"
	"testing"
	"time"
)

func TestJournal(t *testing.T) {
	journal := NewJournal("./journal/", &Config{
		journalSyncData:            false,
		journalWriteData:           true,
		journalAdaptiveGroupWrites: true,
		journalRemoveFromPageCache: true,
	})

	go journal.startJournal()
	time.Sleep(1 * time.Second)

	entry := make([]byte, 1024*5)
	wg := sync.WaitGroup{}
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		journal.logAddEntry(100001, uint64(i), entry, false, &nopWriteCallback{
			cb: func() {
				wg.Done()
			},
		})
	}
	wg.Wait()
}
