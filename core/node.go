package core

import (
	"goactor/message/mm"
	"goactor/structs"
	"math/rand"
	"time"
)

const (
	SnapShotDuration    = time.Minute
	NodeChannelCapacity = 1024
)

type INode interface {
	structs.IId
	Mailbox() chan<- Message
}

type Node struct {
	id             string
	actors         *structs.Map[string, IActor]
	shardActors    *structs.Map[int16, *structs.Set[string]]
	actorKeepAlive *structs.Map[string, time.Time]
	disableActors  *structs.Set[string]
	pauseActors    *structs.Map[string, int64]
	inflight       *structs.Map[int16, *Inflight]
	pauseShards    *structs.Map[int16, int64]
	snapshots      *structs.Map[string, mm.ActorSnapshot]
	mailbox        chan Message
	factory        *Factory
	snapshotTimer  *time.Timer
}

func NewNode() *Node {
	return &Node{
		actors:         structs.NewMap[string, IActor](0),
		shardActors:    structs.NewMap[int16, *structs.Set[string]](0),
		actorKeepAlive: structs.NewMap[string, time.Time](0),
		disableActors:  structs.NewSet[string](0),
		pauseActors:    structs.NewMap[string, int64](0),
		pauseShards:    structs.NewMap[int16, int64](0),
		inflight:       structs.NewMap[int16, *Inflight](0),
		snapshots:      structs.NewMap[string, mm.ActorSnapshot](0),
		mailbox:        make(chan Message, NodeChannelCapacity),
		factory:        NewFactory(),
	}
}

func (n *Node) Id() string {
	return n.id
}

func (n *Node) Mailbox() chan<- Message {
	return n.mailbox
}

func (n *Node) Start() {
	go func() {
		defer func() {
			msg := recover()
			if msg != nil {

			}
		}()

		n.beforeStart()

		for {
			select {
			case msg := <-n.mailbox:
				n.handle(msg)
			case <-n.snapshotTimer.C:
				n.snapshotBatch()
				n.restartTimer()
			}
		}
	}()
}

func (n *Node) beforeStart() {
	n.snapshotTimer = time.NewTimer(SnapShotDuration)
}

func (n *Node) restartTimer() {
	randTime := time.Duration(rand.Int63n(60)) * time.Second
	n.snapshotTimer = time.NewTimer(SnapShotDuration + randTime)
}

func (n *Node) handle(message Message) {
	switch message.Type {
	case MessageTypeMemory:
		actorId := message.Sender.Id
		payload := message.Payload

		switch message.Command {
		case mm.CmdActorIdle:
			n.receiveIdle(actorId, payload.(mm.ActorIdle))
		case mm.CmdActorSnapshot:
			n.receiveSnapshot(actorId, payload.(mm.ActorSnapshot))
		case mm.CmdActorStorage:
			n.receiveStorage(actorId, payload.(mm.ActorStorage))
		case mm.CmdActorAlive:
			n.receiveAlive(actorId, payload.(mm.ActorAlive))
		case mm.CmdActorReady:
			n.receiveReady(actorId, payload.(mm.ActorReady))
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
		newActor, err := n.startActor(message.Receiver)
		if err != nil {
			return
		}
		actor = newActor
	}

	// if actor's channel is full, pause it
	// when it's ready, seek mq message
	select {
	case actor.Mailbox() <- message:
	default:
		n.pauseActors.AddIfNotExist(actorId, offset)
	}
}

func (n *Node) startActor(ref MessageRef) (actor IActor, err error) {
	config := ActorConfigDefault()
	config.Id = ref.Id
	config.Type = ref.Type
	config.Node = n

	actor, err = n.factory.New(config)
	if err != nil {
		return
	}
	err = actor.Start()
	if err != nil {
		return
	}

	n.actors.AddOrUpdate(ref.Id, actor)
	shardId := ActorShard(ref.Id)
	shard, ok := n.shardActors.Get(shardId)
	if !ok {
		shard = structs.NewSet[string](0)
	}
	shard.Add(ref.Id)
	n.shardActors.AddOrUpdate(shardId, shard)
	return
}

func (n *Node) recycleActor(actorId string) {
	if !n.actors.Exist(actorId) {
		return
	}
	// ensure actor's inflight is empty
	// actor can't stop now
	shardId := ActorShard(actorId)
	inflight := n.getInflight(shardId)
	offset, ok := inflight.GetMinOffset(actorId)
	if ok {
		n.seekMessage(shardId, offset)
		return
	}
	// ensure actor's message is all handled
	// actor can't stop now
	offset, ok = n.pauseActors.Get(actorId)
	if ok {
		n.seekMessage(shardId, offset)
		return
	}
	// ensure actor's snapshot is stored
	// if failed, next start can rehandle message
	_, ok = n.snapshots.Get(actorId)
	if ok {
		n.snapshotBatch()
	}

	n.stopActor(actorId)
}

func (n *Node) stopActor(actorId string) {
	actor, exist := n.actors.Get(actorId)
	if !exist {
		return
	}

	shardId := ActorShard(actorId)
	n.pauseActors.Del(actorId)
	n.actorKeepAlive.Del(actorId)
	n.disableActors.Del(actorId)
	n.snapshots.Del(actorId)
	n.actors.Del(actorId)

	shard, ok := n.shardActors.Get(shardId)
	if ok {
		shard.Del(actorId)
		n.shardActors.AddOrUpdate(shardId, shard)
	}
	actor.Stop()
}

func (n *Node) stopShard(shardId int16) {
	shard := n.shardActors.GetDefault(shardId)
	if shard == nil || shard.Len() == 0 {
		return
	}
	actors := shard.All()
	snapshots := make([]mm.ActorSnapshot, 0, len(actors))
	for _, actorId := range actors {
		value, ok := n.snapshots.Get(actorId)
		if !ok {
			continue
		}
		snapshots = append(snapshots, value)
	}
	// TODO mongo and completed
	completed := make([]InflightComplete, 0, len(actors))
	n.completeOffset(shardId, completed...)
	n.inflight.Del(shardId)
}

func (n *Node) receiveIdle(actorId string, message mm.ActorIdle) {
	n.recycleActor(actorId)
}

func (n *Node) receiveSnapshot(actorId string, message mm.ActorSnapshot) {
	n.snapshots.AddOrUpdate(actorId, message)
}

func (n *Node) receiveStorage(actorId string, message mm.ActorStorage) {
	shardId := ActorShard(actorId)
	inflight := n.getInflight(shardId)
	inflight.Complete(InflightComplete{
		ActorId:   actorId,
		MaxOffset: message.Offset,
	})
}

func (n *Node) receiveAlive(actorId string, message mm.ActorAlive) {
	n.actorKeepAlive.AddOrUpdate(actorId, message.Time)
}

func (n *Node) receiveReady(actorId string, message mm.ActorReady) {
	// ready or not is upon to actor not node
	// there is a possible that actor is full but no more message, then ready
	offset, ok := n.pauseActors.Get(actorId)
	if !ok {
		return
	}
	n.pauseActors.Del(actorId)
	shardId := ActorShard(actorId)
	n.seekMessage(shardId, offset)
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
		n.resumeOffset(shardId)
	}
}

func (n *Node) completeOffset(shardId int16, list ...InflightComplete) {
	inflight := n.getInflight(shardId)
	nextOffset, advanced := inflight.Complete(list...)
	if !advanced {
		return
	}
	n.commitOffset(shardId, nextOffset)
}

func (n *Node) resumeOffset(shardId int16) {
	if !n.pauseShards.Exist(shardId) {
		return
	}
	offset := n.pauseShards.GetDefault(shardId)
	n.pauseShards.Del(shardId)
	n.seekMessage(shardId, offset)
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

func (n *Node) seekMessage(shardId int16, offset int64) {
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
