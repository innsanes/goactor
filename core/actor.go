package core

import (
	"context"
	"errors"
	"fmt"
	"goactor/registry"
	"goactor/structure"
)

const (
	ActorChannelCap = 1024
)

type IActor interface {
	structure.IId
	Start() error
	Stop()
	Receive(...Message) error
	AfterStart()
	BeforeStop()
}

type Actor struct {
	id      string
	ch      chan Message
	reg     registry.IRegistry
	ren     registry.IRegistration
	stop    chan struct{}
	node    string
	level   int8
	timer   *Timer
	ctx     context.Context
	cancel  context.CancelFunc
	closing bool
}

func NewActor(id string, node string) *Actor {
	ctx, cancel := context.WithCancel(context.Background())
	return &Actor{
		id:      id,
		ch:      make(chan Message, ActorChannelCap),
		stop:    make(chan struct{}),
		node:    node,
		level:   ActorLevelUnit,
		timer:   NewTimer(),
		ctx:     ctx,
		cancel:  cancel,
		closing: false,
	}
}

func (a *Actor) Id() string {
	return a.id
}
func (a *Actor) SetLevel(level int8) {
	a.level = level
}

func (a *Actor) name() string {
	return fmt.Sprintf("/actor/%s", a.id)
}

func (a *Actor) Start() error {
	err := a.prepare()
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

		a.AfterStart()

		for {
			select {
			case msg := <-a.ch:
				a.handle(msg)
			case <-a.timer.Chan():
				a.handleTimer()
			case <-a.ren.Done():
				a.close()
				return
			case <-a.stop:
				a.close()
				a.drain()
				a.BeforeStop()
				return
			}
		}
	}()

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
	reg, err := a.reg.Register(a.ctx, a.name(), a.node)
	if err != nil {
		a.cancel()
		return err
	}
	a.ren = reg
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
	_ = a.ren.Close()
	a.cancel()
}

func (a *Actor) AfterStart() {
}

func (a *Actor) BeforeStop() {}

func (a *Actor) Receive(msg ...Message) error {
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

	now := Now()
	for {
		item, ok := a.timer.Peek()
		if !ok || item.When > now {
			break
		}

		select {
		case a.ch <- item.Value:
			a.timer.Remove(item.Key)
		default:
			break
		}
	}

	a.timer.Calibration()
	return
}

func (a *Actor) handle(msg Message) {
}
