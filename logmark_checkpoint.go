package bookkeepergo

type logMarkCheckpoint struct {
	mark *lastLogMark
}

func (l *logMarkCheckpoint) compareTo(checkpoint Checkpoint) int {
	if _, ok := checkpoint.(*maxCheckpoint); ok {
		return -1
	}
	if _, ok := checkpoint.(*minCheckpoint); ok {
		return 1
	}
	if o, ok := checkpoint.(*logMarkCheckpoint); ok {
		return l.mark.getCurMark().compare(o.mark.getCurMark())
	}
	return 0
}

func (l *logMarkCheckpoint) equals(checkpoint Checkpoint) bool {
	if o, ok := checkpoint.(*logMarkCheckpoint); ok {
		return 0 == l.compareTo(o)
	}
	return false
}
