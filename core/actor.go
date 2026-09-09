package core

import (
	"context"
	"errors"
	"fmt"
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

type IState any

type TaskHandler func(context.Context, Message)

type Actor[T IState] struct {
	id             string
	typ            string
	mailbox        chan Message
	mailFull       bool
	stop           chan struct{}
	node           INode
	timer          *Timer
	ctx            context.Context
	cancel         context.CancelFunc
	closing        bool
	state          T
	version        int64
	handler        TaskHandler
	idleTime       time.Duration
	idleTimer      *time.Timer
	aliveTimer     *time.Timer
	dedup          *Dedup
	offset         int64
	offsetAdvanced int64
	snapshotAt     time.Time
}

func NewActor[T IState](config ActorConfig) IActor {
	ctx, cancel := context.WithCancel(context.Background())
	actorConfigEnsure(&config)
	actor := &Actor[T]{
		id:      config.Id,
		typ:     config.Type,
		mailbox: make(chan Message, config.ChannelCap),
		stop:    make(chan struct{}),
		node:    config.Node,
		timer:   NewTimer(),
		ctx:     ctx,
		cancel:  cancel,
		closing: false,
		dedup:   NewDedup(config.DedupCap),
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
			a.shutdown()
		}()

		for {
			select {
			case msg := <-a.mailbox:
				a.resetIdle()
				a.handle(msg)
			case <-a.idleTimer.C:
				a.resetIdle()
				a.signalIdle()
			case <-a.aliveTimer.C:
				a.alive()
				a.signalAlive()
			case <-a.timer.Chan():
				a.handleTimer()
			case <-a.stop:
				a.close()
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
	case <-a.stop:
		return
	default:
		close(a.stop)
	}
}

func (a *Actor[T]) prepare() error {
	return nil
}

func (a *Actor[T]) close() {
	a.closing = true
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

func (a *Actor[T]) shutdown() {
	a.cancel()
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
	_ = a.signal(MActorIdle, Idle{})
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
	_ = a.signal(MActorAlive, Alive{
		Time: Now(),
	})
}

func (a *Actor[T]) signalSnapshot() {
	snapshot := Snapshot{
		ActorID: a.id,
		Version: a.version,
		Offset:  a.offset,
		State:   a.state,
		Dedup:   a.dedup.Ids(),
	}
	// allow fail, better not
	_ = a.signal(MActorSnapShot, snapshot)
}

func (a *Actor[T]) signal(cmd string, payload any) error {
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
	select {
	case a.node.Mailbox() <- m:
		return nil
	default:
		return errors.New("send message failed")
	}
}

func (a *Actor[T]) Receive(msg ...Message) error {
	for i := range msg {
		select {
		case a.mailbox <- msg[i]:
		default:
			return fmt.Errorf("actor channel full")
		}
	}
	return nil
}

func (a *Actor[T]) Mailbox() chan<- Message {
	return a.mailbox
}

func (a *Actor[T]) handleTimer() {
	if a.closing == true {
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
		err := a.signal(MActorChannelReady, ChannelReady{})
		if err == nil {
			a.mailFull = false
		}
	}

	switch m.Type {
	case MessageTypeNetwork:
		if a.dedup.Has(m.MessageId) {
			return
		}
	default:
		return
	}

	if a.handler == nil {
		return
	}
	a.handler(a.ctx, m)

	switch m.Type {
	case MessageTypeNetwork:
		a.dedup.Add(m.MessageId)
		a.offset = m.Offset
	default:
		return
	}
}
