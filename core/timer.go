package core

import (
	"goactor/structure"
	"time"
)

type Timer struct {
	timer *time.Timer
	heap  *structure.QuadHeap[string, Message, int64]
}

func NewTimer() *Timer {
	timer := time.NewTimer(time.Second)
	timer.Stop()
	return &Timer{
		timer: timer,
		heap:  structure.NewQuadHeap[string, Message, int64](1),
	}
}

func (t *Timer) Add(key string, m Message, when int64) {
	peek, ok := t.heap.Peek()
	t.heap.Upsert(key, m, when)

	if ok && peek.When <= when {
		return
	}

	t.Calibration()
}

func (t *Timer) Upsert(key string, m Message, when int64) {
	t.heap.Upsert(key, m, when)
}

func (t *Timer) Del(key string) {
	peek, ok := t.heap.Peek()
	t.heap.Remove(key)

	if !ok || peek.Key != key {
		return
	}

	t.Calibration()
}

func (t *Timer) Remove(key string) {
	t.heap.Remove(key)
}

func (t *Timer) Chan() <-chan time.Time {
	return t.timer.C
}

func (t *Timer) Calibration() {
	peek, ok := t.heap.Peek()
	if !ok {
		t.timer.Stop()
		return
	}
	t.timer.Reset(time.Second * time.Duration(peek.When-Now()))
}

func (t *Timer) Peek() (structure.HeapItem[string, Message, int64], bool) {
	return t.heap.Peek()
}
