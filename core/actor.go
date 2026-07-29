package core

import (
	"context"
	"fmt"
	"goactor/registry"
	"goactor/structure"
)

type IActor interface {
	structure.IId
	Start() error
	Stop()
	Receive(Message)
}

type Actor struct {
	id   string
	ch   chan Message
	reg  registry.IRegistry
	stop chan struct{}
	node string
}

func NewActor(id string, node string) *Actor {
	return &Actor{
		id:   id,
		ch:   make(chan Message, 1024),
		stop: make(chan struct{}, 1),
		node: node,
	}
}

func (a *Actor) Id() string {
	return a.id
}

func (a *Actor) name() string {
	return fmt.Sprintf("/actor/%s", a.id)
}

func (a *Actor) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	err := a.reg.Register(ctx, a.name(), a.node, 5)
	if err != nil {
		cancel()
		return err
	}

	go func() {
		defer func() {
			msg := recover()
			if msg != nil {

			}

			close(a.ch)
			cancel()
			_ = a.reg.Close()
		}()

		a.AfterStart()

		for {
			select {
			case msg := <-a.ch:
				a.handle(msg)
			case <-a.stop:
				a.BeforeStop()
				a.closing()
				return
			}
		}
	}()

	return nil
}

func (a *Actor) Stop() {
	a.stop <- struct{}{}
}

func (a *Actor) closing() {
	for {
		select {
		case task := <-a.ch:
			a.handle(task)
		default:
			return
		}
	}
}

func (a *Actor) AfterStart() {
}

func (a *Actor) BeforeStop() {
}

func (a *Actor) Receive(msg Message) error {
	select {
	case a.ch <- msg:
		return nil
	default:
		return fmt.Errorf("actor channel full")
	}
}

func (a *Actor) handle(msg Message) {
}
