package bookkeepergo

import (
	"encoding/binary"
	"fmt"
)

type JournalScanner interface {
	process(journalVersion int, offset int64, entry []byte) error
}

type printfJournalScanner struct {
}

func (j *printfJournalScanner) process(journalVersion int, offset int64, entry []byte) error {
	ledgerid := int(binary.BigEndian.Uint64(entry[0:]))
	entryid := int(binary.BigEndian.Uint64(entry[8:]))
	fmt.Println("ledgerid", ledgerid, "entryid", entryid)
	return nil
}

type EntryLogScanner interface {
	accept(ledgerId int64) bool
	process(ledgerId int64, offset int64, entry []byte) error
}
