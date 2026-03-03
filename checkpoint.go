package bookkeepergo

type Checkpoint interface {
	compareTo(checkpoint Checkpoint) int
	equals(checkpoint Checkpoint) bool
}

var MAX Checkpoint = &maxCheckpoint{}
var MIN Checkpoint = &minCheckpoint{}

type CheckpointSource interface {
	newCheckpoint() Checkpoint
	checkpointComplete(checkpoint Checkpoint, compact bool) error
}

type maxCheckpoint struct {
}

func (m *maxCheckpoint) compareTo(checkpoint Checkpoint) int {
	if _, ok := checkpoint.(*maxCheckpoint); ok {
		return 0
	}
	return 1
}

func (m *maxCheckpoint) equals(checkpoint Checkpoint) bool {
	if _, ok := checkpoint.(*maxCheckpoint); ok {
		return m == checkpoint
	}
	return true
}

type minCheckpoint struct {
}

func (m *minCheckpoint) compareTo(checkpoint Checkpoint) int {
	if _, ok := checkpoint.(*minCheckpoint); ok {
		return 0
	}
	return -1
}

func (m *minCheckpoint) equals(checkpoint Checkpoint) bool {
	if _, ok := checkpoint.(*minCheckpoint); ok {
		return m == checkpoint
	}
	return false
}