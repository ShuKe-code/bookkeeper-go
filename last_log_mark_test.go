package bookkeepergo

import (
	"testing"

	"github.com/c2fo/testify/assert"
)

func TestLastLogMark(t *testing.T) {
	lastLogMark := newLastLogMark(0, 0, newLedgerDirsManager(), "lastMark")
	lastLogMark.setCurLogMark(33, 10002)
	assert.Equal(t, int64(33), lastLogMark.getCurMark().getLogFileId())
	assert.Equal(t, int64(10002), lastLogMark.getCurMark().getLogFileOffset())
	lastLogMark.rollLog()

	lastLogMark = newLastLogMark(0, 0, newLedgerDirsManager(), "lastMark")
	lastLogMark.readLog()
	assert.Equal(t, int64(33), lastLogMark.getCurMark().getLogFileId())
	assert.Equal(t, int64(10002), lastLogMark.getCurMark().getLogFileOffset())
}
