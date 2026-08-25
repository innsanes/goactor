package core

import (
	"context"
	"errors"
	"fmt"
	"goactor/structure"
	"time"
)

const (
	ActorChannelCap = 1024
)

const (
	ActorLevelUnit int8 = iota
)

type IActor interface {
	structure.IId
	Start() error
	Stop()
	Receive(...IMessage) error
}

type IMessage any

type TaskHandler func(context.Context, IMessage)

type Actor struct {
	id        string
	ch        chan IMessage
	stop      chan struct{}
	nodeId    string
	nodeEvent chan<- IMessage
	level     int8
	timer     *Timer
	ctx       context.Context
	cancel    context.CancelFunc
	closing   bool
	state     any
	version   int64
	handler   TaskHandler
	idleTime  time.Duration
	idleTimer *time.Timer
}

func NewActor(config ActorConfig, bs ...ActorBuilder) *Actor {
	ctx, cancel := context.WithCancel(context.Background())
	actor := &Actor{
		id:        config.Id,
		ch:        make(chan IMessage, ActorChannelCap),
		stop:      make(chan struct{}),
		nodeId:    config.NodeId,
		nodeEvent: config.NodeEvent,
		timer:     NewTimer(),
		ctx:       ctx,
		cancel:    cancel,
		closing:   false,
	}
	for _, b := range bs {
		b(actor)
	}
	return actor
}

type ActorConfig struct {
	Id        string
	NodeId    string
	NodeEvent chan<- IMessage
}

type ActorBuilder func(*Actor)

func ActorSetLevel(a *Actor, level int8) {
	a.level = level
}

func ActorSetIdleTime(a *Actor, idleTime time.Duration) {
	a.idleTime = idleTime
}

func (a *Actor) Id() string {
	return a.id
}

func (a *Actor) Start() error {
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

func (a *Actor) Stop() {
	select {
	case <-a.stop:
		return
	default:
		close(a.stop)
	}
}

func (a *Actor) prepare() error {
	return nil
}

func (a *Actor) close() {
	a.closing = true
}

func (a *Actor) drain() {
	for {
		select {
		case task := <-a.ch:
			a.handle(task)
		default:
			return
		}
	}
}

func (a *Actor) shutdown() {
	close(a.ch)
	a.cancel()
}

func (a *Actor) beforeStart() error {
	a.idleTimer = time.NewTimer(a.idleTime)
	return nil
}

func (a *Actor) afterStart() {
}

func (a *Actor) beforeStop() {
}

func (a *Actor) resetIdle() {
	a.idleTimer.Reset(a.idleTime)
}

func (a *Actor) signalIdle() {

}

func (a *Actor) signalSnapshot() {
	snapshot := Snapshot{
		ActorID: a.id,
		Version: a.version,
		Offset:  0,
		State:   nil,
		Dedup:   nil,
	}
	a.signal(snapshot)
}

func (a *Actor) signal(m IMessage) {
	select {
	case a.nodeEvent <- m:
	default:
	}
}

func (a *Actor) Receive(msg ...IMessage) error {
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

func (a *Actor) handleTimer() {
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

func (a *Actor) handle(task IMessage) {
	if a.handler != nil {
		a.handler(a.ctx, task)
	}
}
