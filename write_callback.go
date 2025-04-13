package bookkeepergo

type WriteCallback interface {
	writeComplete(rd int, ledgerId uint64, entryId uint64)
}
