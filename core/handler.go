package core

import "goactor/structs"

type Handlers[T IState] struct {
	reg *structs.Map[string, Handler[T]]
}

type Handler[T IState] func(*Context[T]) error

func NewHandlers[T IState]() *Handlers[T] {
	return &Handlers[T]{
		reg: structs.NewMap[string, Handler[T]](0),
	}
}

func (h *Handlers[T]) Register(cmd string, f Handler[T]) {
	h.reg.AddOrUpdate(cmd, f)
}

func (h *Handlers[T]) GetHandler(cmd string) (Handler[T], bool) {
	return h.reg.Get(cmd)
}
