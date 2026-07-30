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

	t.resetTimer(when)
}

func (t *Timer) Del(key string) {
	peek, ok := t.heap.Peek()
	t.heap.Remove(key)

	if !ok || peek.Key != key {
		return
	}

	newPeek, ok := t.heap.Peek()
	if !ok {
		t.timer.Stop()
		return
	}

	t.resetTimer(newPeek.When)
}

func (t *Timer) Chan() <-chan time.Time {
	return t.timer.C
}

func (t *Timer) Calibration() {
	peek, ok := t.heap.Peek()
	if !ok {
		return
	}
	t.resetTimer(peek.When)
}

func (t *Timer) GetMessages() []Message {
	now := time.Now().Unix() + GlobalTimeOffset
	msg := make([]Message, 0, 1)
	for {
		item, ok := t.heap.Peek()
		if !ok || item.When > now {
			break
		}
		msg = append(msg, item.Value)
		t.heap.Remove(item.Key)
	}
	return msg
}

func (t *Timer) resetTimer(when int64) {
	t.timer.Reset(time.Second * time.Duration(when-Now()))
}
