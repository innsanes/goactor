package core

import (
	"goactor/structure"
	"math/rand"
	"time"
)

const (
	SnapShotDuration = time.Minute
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
	factory        *Factory
	snapshotTimer  *time.Timer
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
		factory:        NewFactory(),
	}
}

func (n *Node) Start() {
	go func() {
		defer func() {
			msg := recover()
			if msg != nil {

			}
		}()

		for {
			select {
			case msg := <-n.channel:
				n.handle(msg)
			case <-n.snapshotTimer.C:
				n.SnapshotBatch()
				n.RestartTimer()
			}
		}
	}()
}

func (n *Node) RestartTimer() {
	randTime := time.Duration(rand.Int63n(60)) * time.Second
	n.snapshotTimer = time.NewTimer(SnapShotDuration + randTime)
}

func (n *Node) handle(message Message) {
	switch message.Type {
	case MessageTypeMemory:
		switch message.Command {
		case MActorIdle:
		case MActorSnapShot:
			n.receiveSnapshot(message.Payload.(Snapshot))
		case MActorStorage:
			n.receiveStorage(message.Payload.(Storage))
		case MActorAlive:
			n.receiveAlive(message.Payload.(Alive))
		default:
			// error
		}
	default:
		// error
	}
}

func (n *Node) Dispatcher(message Message) {
	if n.pause {
		return
	}

	actorId := message.Receiver.Id
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

func (n *Node) startActor(ref MessageRef) error {
	config := ActorConfigDefault()
	config.Id = ref.Id
	config.NodeId = n.id
	config.NodeEvent = n.channel

	actor, err := n.factory.New(config)
	if err != nil {
		return err
	}
	err = actor.Start()
	if err != nil {
		return err
	}

	n.actors[ref.Id] = actor
	return nil
}

func (n *Node) receiveSnapshot(message Snapshot) {
	structure.MapAdd(n.snapshots, message.ActorID, message)
}

func (n *Node) receiveStorage(message Storage) {
	shardId := ActorShard(message.ActorID)
	inflight := n.getOrCreateInflight(shardId)
	inflight.Complete(InflightComplete{
		ActorId:   message.ActorID,
		MaxOffset: message.Offset,
	})
}

func (n *Node) receiveAlive(message Alive) {
	n.actorKeepAlive[message.ActorID] = message.Time
}

func (n *Node) SnapshotBatch() {
	// TODO mongo
	// TODO except storage fail snapshot
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
