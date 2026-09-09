package core

import (
	"goactor/structs"
	"slices"
)

const (
	InflightWindowLimit = 4096
)

type Inflight struct {
	windowLimit  int
	minOffset    int64
	maxOffset    int64
	offsetActor  *structs.Map[int64, string]
	actorOffsets *structs.Map[string, []int64]
}

type InflightComplete struct {
	ActorId   string
	MaxOffset int64
}

func NewInflight(windowLimit int) *Inflight {
	return &Inflight{
		windowLimit:  windowLimit,
		minOffset:    -1,
		maxOffset:    -1,
		offsetActor:  structs.NewMap[int64, string](0),
		actorOffsets: structs.NewMap[string, []int64](0),
	}
}

func (f *Inflight) reset() {
	f.minOffset = -1
	f.maxOffset = -1
	f.offsetActor = structs.NewMap[int64, string](0)
	f.actorOffsets = structs.NewMap[string, []int64](0)
}

func (f *Inflight) Full(offset int64) bool {
	if f.offsetActor.Exist(offset) {
		return false
	}
	if f.maxOffset-f.minOffset >= int64(f.windowLimit) {
		return true
	}
	if f.minOffset != -1 && offset-f.minOffset >= int64(f.windowLimit) {
		return true
	}
	return false
}

func (f *Inflight) Add(offset int64, actorId string) {
	f.offsetActor.AddOrUpdate(offset, actorId)
	if f.minOffset == -1 {
		f.minOffset = offset
	}
	if f.maxOffset == -1 || offset > f.maxOffset {
		f.maxOffset = offset
	}

	if !f.actorOffsets.Exist(actorId) {
		f.actorOffsets.AddIfNotExist(actorId, make([]int64, 0, 1))
	}
	list := f.actorOffsets.GetDefault(actorId)
	if !slices.Contains(list, offset) {
		list = append(list, offset)
		f.actorOffsets.AddOrUpdate(actorId, list)
	}
	return
}

func (f *Inflight) GetMinOffset(actorId string) (offset int64, ok bool) {
	value, ok := f.actorOffsets.Get(actorId)
	if !ok {
		return 0, false
	}
	minOffset := slices.Min(value)
	return minOffset, true
}

func (f *Inflight) Complete(list ...InflightComplete) (nextOffset int64, advanced bool) {
	needAdvance := false
	for _, item := range list {
		need := f.remove(item.ActorId, item.MaxOffset)
		if need {
			needAdvance = true
		}
	}

	if f.offsetActor.Length() == 0 {
		if f.maxOffset < 0 {
			return 0, false
		}

		nextOffset = f.maxOffset + 1
		f.minOffset = -1
		f.maxOffset = -1

		return nextOffset, true
	}

	if needAdvance {
		before := f.minOffset
		f.resetMinOffset()
		nextOffset = f.minOffset
		advanced = nextOffset > before
		return nextOffset, advanced
	}

	return f.minOffset, false
}

func (f *Inflight) remove(actorId string, maxOffset int64) (needAdvance bool) {
	offsets, ok := f.actorOffsets.Get(actorId)
	if !ok || len(offsets) == 0 {
		return
	}
	removed := 0
	for _, offset := range offsets {
		if offset > maxOffset {
			break
		}
		f.offsetActor.Del(offset)
		removed++
		if offset == f.minOffset {
			needAdvance = true
		}
	}
	if removed == len(offsets) {
		f.actorOffsets.Del(actorId)
		return
	}
	offsets = offsets[removed:]
	f.actorOffsets.AddOrUpdate(actorId, offsets)
	return
}

func (f *Inflight) resetMinOffset() {
	newMinOffset := int64(-1)
	for offset := range f.offsetActor.Map() {
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
