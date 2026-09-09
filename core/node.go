package core

import (
	"goactor/structs"
	"math/rand"
	"time"
)

const (
	SnapShotDuration    = time.Minute
	NodeChannelCapacity = 1024
)

type Node struct {
	id             string
	actors         *structs.Map[string, IActor]
	actorKeepAlive *structs.Map[string, time.Time]
	disableActors  *structs.Set[string]
	pauseActors    *structs.Map[string, int64]
	inflight       *structs.Map[int16, *Inflight]
	pauseShards    *structs.Map[int16, int64]
	snapshots      *structs.Map[string, Snapshot]
	channel        chan Message
	factory        *Factory
	snapshotTimer  *time.Timer
}

func NewNode() *Node {
	return &Node{
		actors:         structs.NewMap[string, IActor](0),
		actorKeepAlive: structs.NewMap[string, time.Time](0),
		disableActors:  structs.NewSet[string](0),
		pauseActors:    structs.NewMap[string, int64](0),
		pauseShards:    structs.NewMap[int16, int64](0),
		inflight:       structs.NewMap[int16, *Inflight](0),
		snapshots:      structs.NewMap[string, Snapshot](0),
		channel:        make(chan Message, NodeChannelCapacity),
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
				n.snapshotBatch()
				n.restartTimer()
			}
		}
	}()
}

func (n *Node) restartTimer() {
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
		case MActorChannelReady:
			n.receiveReady(message.Payload.(ChannelReady))
		default:
			// error
		}
	default:
		// error
	}
}

func (n *Node) Dispatcher(message Message) {
	actorId := message.Receiver.Id
	offset := message.Offset
	shardId := ActorShard(actorId)

	if n.pauseShards.Exist(shardId) {
		// logger warn
		return
	}

	inflight := n.getInflight(shardId)
	if inflight.Full(offset) {
		n.pauseShards.AddIfNotExist(shardId, offset)
		n.pausePartition(shardId)
		return
	}

	if n.disableActors.Has(actorId) {
		// TODO DeadLetterQueue
		inflight.Add(offset, actorId)
		n.completeOffset(shardId, InflightComplete{
			ActorId:   actorId,
			MaxOffset: offset,
		})
		return
	}

	if n.pauseActors.Exist(actorId) {
		inflight.Add(offset, actorId)
		return
	}
	inflight.Add(offset, actorId)

	actor, exist := n.actors.Get(actorId)
	if !exist {
		err := n.startActor(message.Receiver)
		if err != nil {
			return
		}
	}

	// if actor's channel is full, pause it
	// when it's ready, seek mq message
	select {
	case actor.Channel() <- message:
	default:
		n.pauseActors.AddIfNotExist(actorId, offset)
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

	n.actors.AddOrUpdate(ref.Id, actor)
	return nil
}

func (n *Node) stopActor(actorId string) {
	actor, exist := n.actors.Get(actorId)
	if !exist {
		return
	}
	// ensure actor's inflight is empty
	// actor can't stop now
	shardId := ActorShard(actorId)
	inflight := n.getInflight(shardId)
	offset, ok := inflight.GetMinOffset(actorId)
	if ok {
		n.seekMessage(offset)
		return
	}
	// ensure actor's message is all handled
	// actor can't stop now
	offset, ok = n.pauseActors.Get(actorId)
	if ok {
		n.seekMessage(offset)
		return
	}
	// ensure actor's snapshot is stored
	// if failed, next start can rehandle message
	_, ok = n.snapshots.Get(actorId)
	if ok {
		n.snapshotBatch()
	}

	n.pauseActors.Del(actorId)
	n.actorKeepAlive.Del(actorId)
	n.disableActors.Del(actorId)
	n.snapshots.Del(actorId)
	n.actors.Del(actorId)
	actor.Stop()
}

func (n *Node) receiveIdle(message Idle) {
	n.stopActor(message.ActorID)
}

func (n *Node) receiveSnapshot(message Snapshot) {
	n.snapshots.AddOrUpdate(message.ActorID, message)
}

func (n *Node) receiveStorage(message Storage) {
	shardId := ActorShard(message.ActorID)
	inflight := n.getInflight(shardId)
	inflight.Complete(InflightComplete{
		ActorId:   message.ActorID,
		MaxOffset: message.Offset,
	})
}

func (n *Node) receiveAlive(message Alive) {
	n.actorKeepAlive.AddOrUpdate(message.ActorID, message.Time)
}

func (n *Node) receiveReady(message ChannelReady) {
	offset, ok := n.pauseActors.Get(message.ActorID)
	if !ok {
		return
	}
	n.pauseActors.Del(message.ActorID)
	n.seekMessage(offset)
}

func (n *Node) snapshotBatch() {
	// TODO mongo
	// TODO except storage fail snapshot
	completed := make(map[int16][]InflightComplete)
	for _, snapshot := range n.snapshots.Map() {
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
		n.completeOffset(shardId, value...)
	}
}

func (n *Node) completeOffset(shardId int16, list ...InflightComplete) {
	inflight := n.getInflight(shardId)
	nextOffset, advanced := inflight.Complete(list...)
	if !advanced {
		return
	}
	n.commitOffset(shardId, nextOffset)
	if !n.pauseShards.Exist(shardId) {
		return
	}
	offset := n.pauseShards.GetDefault(shardId)
	n.pauseShards.Del(shardId)
	n.seekMessage(offset)
}

func (n *Node) getInflight(shardId int16) *Inflight {
	inflight, exist := n.inflight.Get(shardId)
	if exist {
		return inflight
	}

	inflight = NewInflight(InflightWindowLimit)
	n.inflight.AddIfNotExist(shardId, inflight)
	return inflight
}

func (n *Node) seekMessage(offset int64) {
	// TODO
}

func (n *Node) pausePartition(shardId int16) {
	// TODO
}

func (n *Node) resumePartition(shardId int16) {
	// TODO
}

func (n *Node) commitOffset(shardId int16, nextOffset int64) {
	// TODO
}
