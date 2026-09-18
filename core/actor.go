package core

import (
	"context"
	"errors"
	"goactor/message/mm"
	"goactor/structs"
	"time"
)

const (
	ActorChannelCap int = 1024
	ActorDedupCap   int = 512
	AliveTime           = time.Second * 5
	SnapshotTime        = time.Second * 30
	OffsetAdvanced      = 30
)

type IActor interface {
	structs.IId
	Start() error
	Stop()
	Mailbox() chan<- Message
}

const (
	ActorStatusInit int8 = iota
	ActorStatusStarting
	ActorStatusRunning
	ActorStatusStopping
)

type IState any

type Actor[T IState] struct {
	id       string
	typ      string
	mailbox  chan Message
	mailFull bool
	stop     chan struct{}
	node     INode

	// timer is not for logic itself, only a trigger
	// every time actor start should recheck and rebuild timer
	timer *Timer

	ctx            context.Context
	cancel         context.CancelFunc
	state          T
	version        int64
	handler        *Handlers[T]
	idleTime       time.Duration
	idleTimer      *time.Timer
	aliveTimer     *time.Timer
	dedup          *Dedup
	offset         int64
	offsetAdvanced int64
	snapshotAt     time.Time
	status         *structs.Status[int8]
}

func NewActor[T IState](config ActorConfig) *Actor[T] {
	ctx, cancel := context.WithCancel(context.Background())
	actorConfigEnsure(&config)
	actor := &Actor[T]{
		id:      config.Id,
		typ:     config.Type,
		mailbox: make(chan Message, config.ChannelCap),
		stop:    make(chan struct{}, 1),
		node:    config.Node,
		timer:   NewTimer(),
		ctx:     ctx,
		cancel:  cancel,
		dedup:   NewDedup(config.DedupCap),
		status:  structs.NewStatus(ActorStatusInit),
	}
	return actor
}

type ActorConfig struct {
	Id   string
	Type string
	Node INode

	ChannelCap int
	DedupCap   int
}

func ActorConfigDefault() ActorConfig {
	return ActorConfig{
		ChannelCap: ActorChannelCap,
		DedupCap:   ActorDedupCap,
	}
}

func actorConfigEnsure(config *ActorConfig) {
	if config.ChannelCap <= 0 {
		config.ChannelCap = ActorChannelCap
	}
	if config.DedupCap <= 0 {
		config.DedupCap = ActorDedupCap
	}
}

func (a *Actor[T]) Id() string {
	return a.id
}

func (a *Actor[T]) Start() error {
	if !a.status.IsStatus(ActorStatusInit) {
		return errors.New("already started")
	}
	a.status.SetStatus(ActorStatusStarting)
	err := a.prepare()
	if err != nil {
		return err
	}
	err = a.beforeStart()
	if err != nil {
		return err
	}

	go func() {
		defer func() {
			msg := recover()
			if msg != nil {

			}
			a.finish()
		}()

		a.status.SetStatus(ActorStatusRunning)

		for {
			select {
			case msg := <-a.mailbox:
				a.resetIdle()
				a.handle(msg)
			case <-a.idleTimer.C:
				a.resetIdle()
				a.signalSnapshot()
				a.signalIdle()
			case <-a.aliveTimer.C:
				a.alive()
				a.signalAlive()
			case <-a.timer.Chan():
				a.handleTimerTrigger()
			case <-a.stop:
				a.startStopping()
				a.drain()
				a.beforeStop()
				return
			}
		}
	}()

	a.afterStart()

	return nil
}

func (a *Actor[T]) Stop() {
	select {
	case a.stop <- struct{}{}:
		return
	default:
	}
}

func (a *Actor[T]) prepare() error {
	return nil
}

func (a *Actor[T]) startStopping() {
	a.status.SetStatus(ActorStatusStopping)
}

func (a *Actor[T]) drain() {
	for {
		select {
		case task := <-a.mailbox:
			a.handle(task)
		default:
			return
		}
	}
}

func (a *Actor[T]) finish() {
	a.cancel()
	a.signalSnapshot()
	a.signalStopped()
}

func (a *Actor[T]) beforeStart() error {
	a.idleTimer = time.NewTimer(a.idleTime)
	a.aliveTimer = time.NewTimer(AliveTime)
	return nil
}

func (a *Actor[T]) afterStart() {
}

func (a *Actor[T]) beforeStop() {
}

func (a *Actor[T]) resetIdle() {
	a.idleTimer.Reset(a.idleTime)
}

func (a *Actor[T]) signalIdle() {
	a.signal(mm.CmdActorIdle, mm.ActorIdle{})
}

func (a *Actor[T]) alive() {
	now := Now()
	isAdvanced := a.offsetAdvanced > 0
	isAdvancedMuch := a.offsetAdvanced > OffsetAdvanced
	isPastMuch := now.Sub(a.snapshotAt) > SnapshotTime
	if isAdvancedMuch || (isAdvanced && isPastMuch) {
		a.signalSnapshot()
		a.snapshotAt = now
		a.offsetAdvanced = 0
	}
	a.aliveTimer.Reset(AliveTime)
}

func (a *Actor[T]) signalAlive() {
	a.signal(mm.CmdActorAlive, mm.ActorAlive{
		Time: Now(),
	})
}

func (a *Actor[T]) signalStopped() {
	a.signal(mm.CmdActorStopped, mm.ActorStopped{})
}

func (a *Actor[T]) signalSnapshot() {
	snapshot := mm.ActorSnapshot{
		ActorId: a.id,
		Version: a.version,
		Offset:  a.offset,
		State:   a.state,
		Dedup:   a.dedup.Ids(),
	}
	a.signal(mm.CmdActorSnapshot, snapshot)
}

func (a *Actor[T]) signal(cmd string, payload any) {
	m := Message{
		Sender: MessageRef{
			Id:   a.id,
			Type: a.typ,
		},
		Receiver: MessageRef{
			Id:   a.node.Id(),
			Type: "node",
		},
		Type:    MessageTypeMemory,
		Command: cmd,
		Payload: payload,
	}

	// default behavior is blocking
	// and wait for node channel space
	select {
	case a.node.Mailbox() <- m:
		return
	}
}

func (a *Actor[T]) Mailbox() chan<- Message {
	return a.mailbox
}

func (a *Actor[T]) handleTimerTrigger() {
	if a.status.IsStatus(ActorStatusStopping) {
		return
	}

	now := NowUnix()
	for {
		item, ok := a.timer.Peek()
		if !ok || item.When > now {
			break
		}

		select {
		case a.mailbox <- item.Value:
			a.timer.Remove(item.Key)
		default:
			a.timer.RetryAfter(time.Second)
			return
		}
	}

	a.timer.Calibration()
	return
}

func (a *Actor[T]) AddTimer(key string, cmd string, payload any, when int64) {
	messageID := GenerateMessageID(key, a.id, a.id, uint64(when))
	message := Message{
		Sender: MessageRef{
			Id:   a.id,
			Type: a.typ,
		},
		Receiver: MessageRef{
			Id:   a.id,
			Type: a.typ,
		},
		TraceId:   messageID,
		MessageId: messageID,
		Type:      MessageTypeTimer,
		Command:   cmd,
		Payload:   payload,
	}
	a.timer.Add(key, message, when)
}

func (a *Actor[T]) handle(m Message) {
	length := len(a.mailbox)
	if length >= cap(a.mailbox)-1 {
		a.mailFull = true
	}
	if a.mailFull && length <= cap(a.mailbox)/2 {
		a.signal(mm.CmdActorReady, mm.ActorReady{})
		a.mailFull = false
	}

	// TODO Prometheus

	switch m.Type {
	case MessageTypeNetwork:
		a.handleNetwork(m)
	case MessageTypeMemory:
		a.handleMemory(m)
	case MessageTypeTimer:
		a.handleTimer(m)
	default:
		// error
	}
}

func (a *Actor[T]) handleNetwork(m Message) {
	if a.dedup.Has(m.MessageId) {
		return
	}
	handler, ok := a.handler.GetHandler(m.Command)
	if !ok {
		return
	}
	ctx := NewContext(a, m)
	err := handler(ctx)
	if err != nil {
		// TODO DLQ
	}

	a.dedup.Add(m.MessageId)
	a.offset = m.Offset
}

func (a *Actor[T]) handleMemory(m Message) {
}

func (a *Actor[T]) handleTimer(m Message) {
	handler, ok := a.handler.GetHandler(m.Command)
	if !ok {
		// logger
		return
	}
	ctx := NewContext(a, m)
	err := handler(ctx)
	_ = err
	// logger
}
