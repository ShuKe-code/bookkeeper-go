package bookkeepergo

import (
	"encoding/binary"
	"sync"
	"testing"
	"time"
)

func TestJournal(t *testing.T) {
	journal := NewJournal(0, "./journal/", &Config{
		journalSyncData:            false,
		journalWriteData:           true,
		journalAdaptiveGroupWrites: true,
		journalRemoveFromPageCache: true,
	}, newLedgerDirsManager())

	go journal.startJournal()
	time.Sleep(1 * time.Second)

	entryData := []byte(`{
	  "key": "1679438886",
	  "meta_data": {
	    "born_app": "live.anchor-inner-ecology.veigar",
	    "born_time": "1744601460927",
	    "born_zone": "sh001",
	    "caller": "live.live.live-streaming",
	    "color": "auto_test",
	    "compress_type": "none",
	    "criticality": "CRITICAL_PLUS",
	    "full_method": "/live.live.avalon.v1.RoomCoreCommand/RoomStatusNotify",
	    "origin_zone": "sh001",
	    "remote_ip": "10.149.61.113",
	    "trace_id": "47b2ccb29ec64aec77e6d1042967fc81:49c639c9d4ef38d3:1ab847d518232633:1",
	    "zone": "sh001"
	  },
	  "value": {
	    "msg_id": "1679438886:veigar:1744601460923865107",
	    "version": 1,
	    "tag_id": 10000003,
	    "oid_type": 1,
	    "oid": "1679438886",
	    "tag_value": 1,
	    "biz_type": "veigar",
	    "start_time": 1744601460,
	    "end_time": -1,
	    "extra": "",
	    "action_type": 0,
	    "receive_time": 1744601460
	  },
	  "timestamp": 1744601460936,
	  "timestamp_string": "2025-04-14 11:31:00",
	  "partition": 0,
	  "offset": 0,
	  "topic": "Veigar-Tag-Change-T"
	}`)
	ledgerId := 100001
	wg := sync.WaitGroup{}

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		entry := make([]byte, 16)
		binary.BigEndian.PutUint64(entry[0:], uint64(ledgerId))
		binary.BigEndian.PutUint64(entry[8:], uint64(i))
		entry = append(entry, entryData...)
		journal.logAddEntry(100001, uint64(i), entry, false, &nopWriteCallback{
			cb: func() {
				wg.Done()
			},
		})
	}
	wg.Wait()
	ids, _ := ListJournalIds("./journal/", nil)
	for _, id := range ids {
		journal.scanJournal(id, START_OF_FILE, &printfJournalScanner{}, false)
	}
}
