package core

import (
	"goactor/structure"
	"time"
)

type Node struct {
	id             string
	actors         map[string]IActor
	actorKeepAlive map[string]time.Time
	disableActors  *structure.Set[string]
	pauseActors    map[string]int64
	inflight       map[int16]*Inflight
	snapshots      map[string]Snapshot
	channel        chan Message
	pause          bool
	pauseOffset    int64
	actorRegistry  *ActorRegistry
}

func NewNode() *Node {
	return &Node{
		actors:         make(map[string]IActor),
		actorKeepAlive: make(map[string]time.Time),
		disableActors:  structure.NewSet[string](0),
		pauseActors:    make(map[string]int64),
		inflight:       make(map[int16]*Inflight),
		snapshots:      make(map[string]Snapshot),
		channel:        make(chan Message),
		actorRegistry:  NewActorRegistry(),
	}
}

func (n *Node) Start() {
}

func (n *Node) Dispatcher(message Message) {
	if n.pause {
		return
	}

	actorId := message.Receiver
	offset := message.Offset
	shardId := ActorShard(actorId)
	inflight := n.getOrCreateInflight(shardId)
	isFull := inflight.Full(offset)
	if isFull {
		// TODO Pause Partition
		n.pause = true
		n.pauseOffset = offset
		return
	}

	if n.disableActors.Has(actorId) {
		// TODO DeadLetterQueue
		inflight.Add(offset, actorId)
		inflight.Complete(InflightComplete{
			ActorId:   actorId,
			MaxOffset: offset,
		})
		return
	}

	if _, exist := n.pauseActors[actorId]; exist {
		inflight.Add(offset, actorId)
		return
	}
	inflight.Add(offset, actorId)

	actor, exist := n.actors[actorId]
	if !exist {
		// TODO start actor
		err := n.startActor(message.Receiver)
		if err != nil {
			return
		}
	}

	select {
	case actor.Channel() <- message:
	default:
		structure.MapAddIfNotExist(n.pauseActors, actorId, offset)
	}
}

func (n *Node) startActor(actorId string) error {
	config := ActorConfigDefault()
	config.Id = actorId
	config.NodeId = n.id
	config.NodeEvent = n.channel

	f, err := n.actorRegistry.GetFunc(actorId)
	if err != nil {
		return err
	}
	actor := f(config)
	err = actor.Start()
	if err != nil {
		return err
	}

	n.actors[actorId] = actor
	return nil
}

func (n *Node) receiveSnapshot(snapshot Snapshot) {
	structure.MapAdd(n.snapshots, snapshot.ActorID, snapshot)
}

func (n *Node) receiveStorage(actorId string, offset int64) {
	shardId := ActorShard(actorId)
	inflight := n.getOrCreateInflight(shardId)
	inflight.Complete(InflightComplete{
		ActorId:   actorId,
		MaxOffset: offset,
	})
}

func (n *Node) SnapshotBatch() {
	// TODO mongo
	// except storage fail snapshot
	completed := make(map[int16][]InflightComplete)
	for _, snapshot := range n.snapshots {
		shardId := ActorShard(snapshot.ActorID)
		if _, ok := completed[shardId]; !ok {
			completed[shardId] = make([]InflightComplete, 0, 1)
		}
		completed[shardId] = append(completed[shardId], InflightComplete{
			ActorId:   snapshot.ActorID,
			MaxOffset: snapshot.Offset,
		})
	}
	for shardId, value := range completed {
		inflight := n.getOrCreateInflight(shardId)
		inflight.Complete(value...)
	}
}

func (n *Node) getOrCreateInflight(shardId int16) *Inflight {
	inflight, exist := n.inflight[shardId]
	if exist {
		return inflight
	}

	inflight = NewInflight(InflightWindowLimit)
	n.inflight[shardId] = inflight
	return inflight
}

func (n *Node) setIfNotExistPauseActor(actorId string, offset int64) {
	_, exist := n.pauseActors[actorId]
	if exist {
		return
	}
	n.pauseActors[actorId] = offset
}
