package structure

import "sync"

type SyncMap[T IId] struct {
	m sync.Map
}

func NewSyncMap[T IId]() *SyncMap[T] {
	return &SyncMap[T]{
		m: sync.Map{},
	}
}

func (mm *SyncMap[T]) Add(item T) {
	mm.m.Store(item.Id(), item)
}

func (mm *SyncMap[T]) Get(id string) (T, bool) {
	user, ok := mm.m.Load(id)
	if !ok {
		var item T
		return item, false
	}
	return user.(T), ok
}

func (mm *SyncMap[T]) Del(id string) {
	mm.m.Delete(id)
}

func (mm *SyncMap[T]) GetAll() []T {
	list := make([]T, 0)
	mm.m.Range(func(key, value any) bool {
		list = append(list, value.(T))
		return true
	})
	return list
}
