package core

import (
	"goactor/message/mm"
	"goactor/structs"
	"time"
)

type NodeActor struct {
	IActor
	keepAlive time.Time
	stopping  bool
	snapshot  *structs.BoolValue[mm.ActorSnapshot]
	pause     *structs.BoolValue[int64]
	pending   *structs.BoolValue[int64]
}

func NewNodeActor(actor IActor) *NodeActor {
	return &NodeActor{
		IActor:    actor,
		keepAlive: Now(),
		snapshot:  structs.NewBoolValue[mm.ActorSnapshot](),
		stopping:  false,
		pause:     structs.NewBoolValue[int64](),
		pending:   structs.NewBoolValue[int64](),
	}
}
