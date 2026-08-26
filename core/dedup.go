package core

import "goactor/structure"

type Dedup struct {
	queue  *structure.Ring[string]
	queued *structure.Set[string]
}

func NewDedup(limit int) *Dedup {
	return &Dedup{
		queue:  structure.NewRing[string](limit),
		queued: structure.NewSet[string](limit),
	}
}

func (d *Dedup) Add(id string) bool {
	if d.queued.Has(id) {
		return false
	}

	if d.queue.Full() {
		value, ok := d.queue.Pop()
		if ok {
			d.queued.Del(value)
		}
	}

	d.queue.Push(id)
	d.queued.Add(id)
	return true
}

func (d *Dedup) Has(id string) bool {
	return d.queued.Has(id)
}

func (d *Dedup) Reset() {
	d.queue.Clear()
	d.queued.Clear()
}

func (d *Dedup) IDs() []string {
	return d.queue.Values()
}

func (d *Dedup) Restore(ids []string) {
	d.Reset()
	for _, id := range ids {
		d.Add(id)
	}
}
