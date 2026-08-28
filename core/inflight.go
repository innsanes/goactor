package core

import (
	"errors"
	"goactor/structure"
)

type Inflight struct {
	capLimit     int
	windowLimit  int
	minOffset    int64
	maxOffset    int64
	offsetActor  map[int64]string
	actorOffsets map[string][]int64
	dirty        bool
}

type InflightComplete struct {
	ActorId   string
	MaxOffset int64
}

func NewInflight(capLimit int, windowLimit int) *Inflight {
	return &Inflight{
		capLimit:     capLimit,
		windowLimit:  windowLimit,
		minOffset:    -1,
		maxOffset:    -1,
		offsetActor:  make(map[int64]string, capLimit),
		actorOffsets: make(map[string][]int64),
	}
}

func (f *Inflight) full(offset int64) bool {
	if structure.MapExist(f.offsetActor, offset) {
		return false
	}
	if len(f.offsetActor) >= f.capLimit {
		return true
	}
	if f.maxOffset-f.minOffset >= int64(f.windowLimit) {
		return true
	}
	if f.minOffset != -1 && offset-f.minOffset >= int64(f.windowLimit) {
		return true
	}
	return false
}

func (f *Inflight) Add(offset int64, actorId string) (err error) {
	if structure.MapExist(f.offsetActor, offset) {
		return nil
	}
	if f.full(offset) {
		return errors.New("inflight is full")
	}

	f.offsetActor[offset] = actorId
	if f.minOffset == -1 {
		f.minOffset = offset
	}
	if f.maxOffset == -1 || offset > f.maxOffset {
		f.maxOffset = offset
	}

	if !structure.MapExist(f.actorOffsets, actorId) {
		f.actorOffsets[actorId] = make([]int64, 0, 1)
	}
	f.actorOffsets[actorId] = append(f.actorOffsets[actorId], offset)
	return nil
}

func (f *Inflight) Complete(list ...InflightComplete) (nextOffset int64, advanced bool) {
	f.dirty = false

	for _, item := range list {
		f.remove(item.ActorId, item.MaxOffset)
	}

	if len(f.offsetActor) == 0 {
		if f.maxOffset < 0 {
			return 0, false
		}

		nextOffset = f.maxOffset + 1
		f.minOffset = -1
		f.maxOffset = -1
		f.dirty = false

		return nextOffset, true
	}

	if f.dirty {
		before := f.minOffset
		f.resetMinOffset()
		nextOffset = f.minOffset
		advanced = nextOffset > before
		f.dirty = false
		return nextOffset, advanced
	}

	return f.minOffset, false
}

func (f *Inflight) remove(actorId string, maxOffset int64) {
	offsets, ok := f.actorOffsets[actorId]
	if !ok || len(offsets) == 0 {
		return
	}
	removed := 0
	for _, offset := range offsets {
		if offset > maxOffset {
			break
		}
		delete(f.offsetActor, offset)
		removed++
		if offset == f.minOffset {
			f.dirty = true
		}
	}
	if removed == len(offsets) {
		delete(f.actorOffsets, actorId)
		return
	}
	offsets = offsets[removed:]
	f.actorOffsets[actorId] = offsets
}

func (f *Inflight) resetMinOffset() {
	newMinOffset := int64(-1)
	for offset := range f.offsetActor {
		if newMinOffset == -1 || offset < newMinOffset {
			newMinOffset = offset
		}
	}
	if newMinOffset == -1 {
		f.minOffset = -1
		return
	}
	f.minOffset = newMinOffset
	return
}
