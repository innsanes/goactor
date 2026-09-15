package core

import "goactor/structs"

type NodeShard struct {
	actors   *structs.Set[string]
	inflight *Inflight
	pause    *structs.BoolValue[int64]
}

func NewNodeShard() *NodeShard {
	return &NodeShard{
		actors:   structs.NewSet[string](0),
		inflight: NewInflight(InflightWindowLimit),
		pause:    structs.NewBoolValue[int64](),
	}
}
