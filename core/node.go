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
	NodeStopTimeout     = time.Second * 10
)

type INode interface {
	structs.IId
	Mailbox() chan<- Message
}

type Node struct {
	id             string
	registerShards *structs.Set[int16]
	shards         *structs.Map[int16, *NodeShard]
	actors         *structs.Map[string, *NodeActor]
	disableActors  *structs.Set[string]
	mailbox        chan Message
	factory        *Factory
	snapshotTimer  *time.Timer
	stop           chan struct{}
	end            chan struct{}
	endTimer       *time.Timer
	stopping       bool
}

func NewNode() *Node {
	return &Node{
		registerShards: structs.NewSet[int16](0),
		shards:         structs.NewMap[int16, *NodeShard](0),
		actors:         structs.NewMap[string, *NodeActor](0),
		disableActors:  structs.NewSet[string](0),
		mailbox:        make(chan Message, NodeChannelCapacity),
		factory:        NewFactory(),
		stop:           make(chan struct{}, 1),
		end:            make(chan struct{}, 1),
		stopping:       false,
	}
}

func (n *Node) Id() string {
	return n.id
}

func (n *Node) Mailbox() chan<- Message {
	return n.mailbox
}

func (n *Node) Start() {
	err := n.beforeStart()
	if err != nil {
		return
	}

	go func() {
		defer func() {
			msg := recover()
			if msg != nil {

			}
			n.finish()
		}()

		for {
			select {
			case msg := <-n.mailbox:
				n.handle(msg)
			case <-n.snapshotTimer.C:
				n.snapshotBatch()
				n.restartTimer()
			case <-n.stop:
				n.beforeStop()
			case <-n.end:
				n.afterStop()
				return
			}
		}
	}()
}

func (n *Node) beforeStart() error {
	n.snapshotTimer = time.NewTimer(SnapShotDuration)
	return nil
}

func (n *Node) beforeStop() {
	if n.stopping {
		return
	}
	n.stopping = true
	// TODO mq and others
	n.registerShards.Clear()
	for _, shardId := range n.shards.Keys() {
		n.stopShard(shardId)
	}
	n.handleEnding()

	n.endTimer = time.AfterFunc(NodeStopTimeout, func() {
		n.end <- struct{}{}
	})
}

func (n *Node) afterStop() {
	n.endTimer.Stop()
	n.snapshotBatch()
}

func (n *Node) finish() {
	n.snapshotTimer.Stop()
}

func (n *Node) handleEnding() {
	if n.actors.Length() == 0 {
		n.end <- struct{}{}
		return
	}
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
		case mm.CmdActorAlive:
			n.receiveAlive(actorId, payload.(mm.ActorAlive))
		case mm.CmdActorReady:
			n.receiveReady(actorId, payload.(mm.ActorReady))
		case mm.CmdActorStopped:
			n.receiveStopped(actorId, payload.(mm.ActorStopped))
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
	if !n.registerShards.Has(shardId) {
		return
	}
	shard, exist := n.shards.Get(shardId)
	if !exist {
		return
	}

	if shard.pause.IsEnabled() {
		// logger
		return
	}

	if shard.inflight.Full(offset) {
		shard.pause.Enable(offset)
		n.pausePartition(shardId)
		return
	}

	if n.disableActors.Has(actorId) {
		// TODO DeadLetterQueue
		shard.inflight.Add(offset, actorId)
		n.completeOffset(shardId, InflightComplete{
			ActorId:   actorId,
			MaxOffset: offset,
		})
		return
	}

	actor, exist := n.actors.Get(actorId)
	if !exist {
		newActor, err := n.startActor(message.Receiver)
		if err != nil {
			return
		}
		actor = newActor
	}

	shard.inflight.Add(offset, actorId)

	// actor must restart to handle this message
	// and new actor must wait for the old to stop
	if actor.stopping {
		actor.pending.Enable(offset)
		return
	}

	if actor.pause.IsEnabled() {
		return
	}

	// if actor's channel is full, pause it
	// when it's ready, seek mq message
	select {
	case actor.Mailbox() <- message:
	default:
		actor.pause.Enable(offset)
	}
}

func (n *Node) startActor(ref MessageRef) (newActor *NodeActor, err error) {
	config := ActorConfigDefault()
	config.Id = ref.Id
	config.Type = ref.Type
	config.Node = n

	actor, err := n.factory.New(config)
	if err != nil {
		return
	}
	err = actor.Start()
	if err != nil {
		return
	}

	newActor = NewNodeActor(actor)
	n.actors.AddOrUpdate(ref.Id, newActor)

	shardId := ActorShard(ref.Id)
	shard := n.shards.GetDefault(shardId)
	shard.actors.Add(ref.Id)
	return
}

func (n *Node) recycleActor(actorId string) {
	actor, exist := n.actors.Get(actorId)
	if !exist {
		return
	}
	// ensure actor's inflight is empty
	// actor can't stop now
	shardId := ActorShard(actorId)
	shard := n.shards.GetDefault(shardId)
	offset, ok := shard.inflight.GetMinOffset(actorId)
	if ok {
		n.seekMessage(shardId, offset)
		return
	}
	// ensure actor's message is all handled
	// actor can't stop now
	offset, ok = actor.pause.Get()
	if ok {
		n.seekMessage(shardId, offset)
		return
	}
	// ensure actor's snapshot is stored
	// if failed, next start can rehandle message
	if actor.snapshot.IsEnabled() {
		n.snapshotBatch()
	}

	n.stopActor(actorId)
}

func (n *Node) stopActor(actorId string) {
	actor, exist := n.actors.Get(actorId)
	if !exist {
		return
	}
	actor.stopping = true
	actor.Stop()
}

func (n *Node) startShard(shardId int16) {
	n.registerShards.Add(shardId)
	if n.shards.Exist(shardId) {
		return
	}
	newShard := NewNodeShard()
	n.shards.AddOrUpdate(shardId, newShard)
}

func (n *Node) stopShard(shardId int16) {
	shard, exist := n.shards.Get(shardId)
	if !exist {
		return
	}

	// send stop message to all shard actors
	actors := shard.actors.All()
	for _, actorId := range actors {
		n.stopActor(actorId)
	}
}

func (n *Node) receiveIdle(actorId string, message mm.ActorIdle) {
	n.recycleActor(actorId)
}

func (n *Node) receiveSnapshot(actorId string, message mm.ActorSnapshot) {
	actor, ok := n.actors.Get(actorId)
	if !ok {
		return
	}
	actor.snapshot.UpdateEnable(message)
}

func (n *Node) receiveAlive(actorId string, message mm.ActorAlive) {
	actor, ok := n.actors.Get(actorId)
	if !ok {
		return
	}
	actor.keepAlive = message.Time
}

func (n *Node) receiveReady(actorId string, message mm.ActorReady) {
	// ready or not is upon to actor, not node
	// there is a possibility that actor is full but no more message, then ready
	actor, ok := n.actors.Get(actorId)
	if !ok {
		return
	}
	offset, isEnabled := actor.pause.Get()
	if !isEnabled {
		return
	}
	actor.pause.Disable()
	shardId := ActorShard(actorId)
	n.seekMessage(shardId, offset)
}

func (n *Node) receiveStopped(actorId string, message mm.ActorStopped) {
	actor, ok := n.actors.Get(actorId)
	if !ok {
		return
	}

	// snapshot must be done after stop
	n.snapshot(actorId)

	n.disableActors.Del(actorId)
	n.actors.Del(actorId)
	shardId := ActorShard(actorId)
	shard, exist := n.shards.Get(shardId)
	if exist {
		shard.actors.Del(actorId)
		if shard.actors.Len() == 0 && !n.registerShards.Has(shardId) {
			n.shards.Del(shardId)
		}
	}

	// node is stopping, wait for all actor stop
	if n.stopping {
		n.handleEnding()
		return
	}

	offset, pending := actor.pending.Get()
	if !pending {
		return
	}
	actor.pending.Disable()

	// new message comes when actor is stopping
	// need restart actor to handle this message
	// seek message will restart it
	n.seekMessage(shardId, offset)
}

func (n *Node) snapshot(actorId string) {
	actor, exist := n.actors.Get(actorId)
	if !exist {
		return
	}
	snapshot, changed := actor.snapshot.Get()
	if !changed {
		return
	}
	// TODO mongo
	// TODO except storage fail snapshot
	actor.snapshot.Disable()
	shardId := ActorShard(actor.Id())
	n.completeOffset(shardId, InflightComplete{
		ActorId:   snapshot.ActorId,
		MaxOffset: snapshot.Offset,
	})
	n.resumeOffset(shardId)
}

func (n *Node) snapshotBatch() {
	actors := n.actors.Map()
	snapshots := make([]mm.ActorSnapshot, 0, len(actors))
	for _, actor := range actors {
		snapshot, isEnabled := actor.snapshot.Get()
		if !isEnabled {
			continue
		}
		snapshots = append(snapshots, snapshot)
	}
	// TODO mongo
	// TODO except storage fail snapshot
	completed := make(map[int16][]InflightComplete)
	for _, actor := range actors {
		snapshot, isEnabled := actor.snapshot.Get()
		if !isEnabled {
			continue
		}
		actor.snapshot.Disable()

		shardId := ActorShard(actor.Id())
		if _, ok := completed[shardId]; !ok {
			completed[shardId] = make([]InflightComplete, 0, 1)
		}

		completed[shardId] = append(completed[shardId], InflightComplete{
			ActorId:   snapshot.ActorId,
			MaxOffset: snapshot.Offset,
		})
	}
	for shardId, value := range completed {
		n.completeOffset(shardId, value...)
		n.resumeOffset(shardId)
	}
}

func (n *Node) completeOffset(shardId int16, list ...InflightComplete) {
	shard, exist := n.shards.Get(shardId)
	if !exist {
		return
	}
	nextOffset, advanced := shard.inflight.Complete(list...)
	if !advanced {
		return
	}
	n.commitOffset(shardId, nextOffset)
}

func (n *Node) resumeOffset(shardId int16) {
	shard, exist := n.shards.Get(shardId)
	if !exist {
		return
	}
	offset, isEnabled := shard.pause.Get()
	if !isEnabled {
		return
	}
	shard.pause.Disable()
	n.seekMessage(shardId, offset)
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
