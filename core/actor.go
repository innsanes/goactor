package core

import (
	"context"
	"errors"
	"fmt"
	"goactor/structure"
	"time"
)

const (
	ActorChannelCap int = 1024
	ActorDedupCap   int = 512
)

type IActor interface {
	structure.IId
	Start() error
	Stop()
	Receive(...Message) error
}

type IState any

type TaskHandler func(context.Context, Message)

type Actor[T IState] struct {
	id        string
	ch        chan Message
	stop      chan struct{}
	nodeId    string
	nodeEvent chan<- Message
	timer     *Timer
	ctx       context.Context
	cancel    context.CancelFunc
	closing   bool
	state     T
	version   int64
	handler   TaskHandler
	idleTime  time.Duration
	idleTimer *time.Timer
	dedup     *Dedup
	offset    int64
}

func NewActor[T IState](config ActorConfig, bs ...ActorBuilder[T]) *Actor[T] {
	ctx, cancel := context.WithCancel(context.Background())
	actor := &Actor[T]{
		id:        config.Id,
		ch:        make(chan Message, config.ChannelCap),
		stop:      make(chan struct{}),
		nodeId:    config.NodeId,
		nodeEvent: config.NodeEvent,
		timer:     NewTimer(),
		ctx:       ctx,
		cancel:    cancel,
		closing:   false,
		dedup:     NewDedup(config.DedupCap),
	}
	for _, b := range bs {
		b(actor)
	}
	return actor
}

type ActorConfig struct {
	Id        string
	NodeId    string
	NodeEvent chan<- Message

	ChannelCap int
	DedupCap   int
}

type ActorBuilder[T IState] func(*Actor[T])

func ActorConfigDefault() *ActorConfig {
	return &ActorConfig{
		ChannelCap: ActorChannelCap,
		DedupCap:   ActorDedupCap,
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
			case msg := <-a.ch:
				a.resetIdle()
				a.handle(msg)
			case <-a.idleTimer.C:
				a.resetIdle()
				a.signalIdle()
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
		case task := <-a.ch:
			a.handle(task)
		default:
			return
		}
	}
}

func (a *Actor[T]) shutdown() {
	close(a.ch)
	a.cancel()
}

func (a *Actor[T]) beforeStart() error {
	a.idleTimer = time.NewTimer(a.idleTime)
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
	a.signal(MActorIdle, Idle{})
}

func (a *Actor[T]) signalSnapshot() {
	snapshot := Snapshot{
		ActorID: a.id,
		Version: a.version,
		Offset:  a.offset,
		State:   a.state,
		Dedup:   a.dedup.Ids(),
	}
	a.signal(MActorSnapShot, snapshot)
}

func (a *Actor[T]) signal(cmd string, payload any) {
	m := Message{
		Sender:   a.id,
		Receiver: a.nodeId,
		Type:     MessageTypeMemory,
		Command:  cmd,
		Payload:  payload,
	}
	select {
	case a.nodeEvent <- m:
	default:
	}
}

func (a *Actor[T]) Receive(msg ...Message) error {
	if a.closing == true {
		return errors.New("actor is closing")
	}
	for i := range msg {
		select {
		case a.ch <- msg[i]:
		default:
			return fmt.Errorf("actor channel full")
		}
	}
	return nil
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
		case a.ch <- item.Value:
			a.timer.Remove(item.Key)
		default:
			a.timer.RetryAfter(time.Second)
			return
		}
	}

	a.timer.Calibration()
	return
}

func (a *Actor[T]) handle(task Message) {
	if a.handler == nil {
		return
	}
	a.handler(a.ctx, task)
}

func (a *Actor[T]) AddTimer(key string, cmd string, payload any, when int64) {
	messageID := GenerateMessageID(key, a.id, a.id, uint64(when))
	message := Message{
		Sender:    a.id,
		Receiver:  a.id,
		TraceId:   messageID,
		MessageId: messageID,
		Type:      MessageTypeTimer,
		Command:   cmd,
		Payload:   payload,
	}
	a.timer.Add(key, message, when)
}
