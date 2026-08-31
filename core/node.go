package core

type Node struct {
	id        string
	actors    map[string]chan<- Message
	inflight  map[int16]Inflight
	snapshots map[string]Snapshot
	shards    []int16
}

func NewNode(id string) *Node {
	return &Node{}
}
