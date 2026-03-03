package bookkeepergo

type WriteCallback interface {
	writeComplete(rd int, ledgerId uint64, entryId uint64)
}

type nopWriteCallback struct {
	cb func()
}

func (nwc *nopWriteCallback) writeComplete(rd int, ledgerId uint64, entryId uint64) {
	nwc.cb()
	// fmt.Println("write complete, ledgerId:", ledgerId, "entryId:", entryId)
}
