package core

import (
	"goactor/structure"
	"time"
)

type Timer struct {
	timer *time.Timer
	heap  *structure.QuadHeap[string, Message, int64]
}

type TimerData struct {
	key  string
	msg  Message
	when int64
}

func NewTimer() *Timer {
	timer := time.NewTimer(time.Second)
	timer.Stop()
	return &Timer{
		timer: timer,
		heap:  structure.NewQuadHeap[string, Message, int64](1),
	}
}

func (t *Timer) Add(key string, task Message, when int64) {
	peek, ok := t.heap.Peek()
	t.heap.Upsert(key, task, when)

	if ok && peek.When <= when {
		return
	}

	t.Calibration()
}

func (t *Timer) Upsert(key string, task Message, when int64) {
	t.heap.Upsert(key, task, when)
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

// RetryAfter defers another attempt to enqueue an expired timer without a busy
// loop when the Actor mailbox is full.
func (t *Timer) RetryAfter(delay time.Duration) {
	if delay <= 0 {
		delay = time.Second
	}
	t.timer.Reset(delay)
}

func (t *Timer) Calibration() {
	peek, ok := t.heap.Peek()
	if !ok {
		t.timer.Stop()
		return
	}
	t.timer.Reset(time.Second * time.Duration(peek.When-NowUnix()))
}

func (t *Timer) Peek() (structure.HeapItem[string, Message, int64], bool) {
	return t.heap.Peek()
}

func (t *Timer) All() []structure.HeapItem[string, Message, int64] {
	return []structure.HeapItem[string, Message, int64]{}
}
