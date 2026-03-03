package bookkeepergo

import (
	"fmt"
	"os"
	"path"
)

type lastLogMark struct {
	curMark           *logMark
	ledgerDirsManager *ledgerDirsManager
	lastMarkFileName  string
}

func newLastLogMark(logId, logPosition int64, ledgerDirsManager *ledgerDirsManager, lastMarkFileName string) *lastLogMark {
	lm := &logMark{}
	lm.setLogMark(uint64(logId), uint64(logPosition))
	lmm := &lastLogMark{curMark: lm, ledgerDirsManager: ledgerDirsManager, lastMarkFileName: lastMarkFileName}
	return lmm
}

func (lmm *lastLogMark) setCurLogMark(logId, logPosition int64) {
	lmm.curMark.setLogMark(uint64(logId), uint64(logPosition))
}

func (lmm *lastLogMark) markLog() *lastLogMark {
	return newLastLogMark(int64(lmm.curMark.getLogFileId()), int64(lmm.curMark.getLogFileOffset()), lmm.ledgerDirsManager, lmm.lastMarkFileName)
}

func (lmm *lastLogMark) getCurMark() *logMark {
	return lmm.curMark
}

func (lmm *lastLogMark) rollLog() error {
	bb := make([]byte, 16)
	lmm.curMark.writeLogMark(bb)

	log.Debug("RollLog to persist last marked log : %d, %d", lmm.curMark.logFileId.Load(), lmm.curMark.logFileOffset.Load())
	writableLedgerDirs := lmm.ledgerDirsManager.getWritableLedgerDirs()

	for _, writableLedgerDir := range writableLedgerDirs {
		filename := path.Join(writableLedgerDir, lmm.lastMarkFileName)

		tmpFile := fmt.Sprintf("%s.tmp", filename)

		if err := os.WriteFile(tmpFile, bb, 0644); err != nil {
			return fmt.Errorf("write tmpFile file: %w", err)
		}

		if err := os.Rename(tmpFile, filename); err != nil {
			return fmt.Errorf("rename tmpFile file: %w", err)
		}
	}
	return nil
}

func (lmm *lastLogMark) readLog() {
	mark := &logMark{}
	for _, writableLedgerDir := range lmm.ledgerDirsManager.getWritableLedgerDirs() {
		filename := path.Join(writableLedgerDir, lmm.lastMarkFileName)
		if data, err := os.ReadFile(filename); err == nil && len(data) == 16 {
			mark.readLogMark(data)
			if lmm.curMark.compare(mark) < 0 {
				lmm.curMark.setLogMark(mark.getLogFileId(), mark.getLogFileOffset())
			}
		}
	}
}
