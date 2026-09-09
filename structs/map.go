package structs

func MapIncrease[K comparable, V int | int8 | int16 | int32 | int64](m map[K]V, key K, add V) (result V) {
	if m == nil {
		m = make(map[K]V)
	}
	_, ok := m[key]
	if !ok {
		m[key] = add
	} else {
		m[key] += add
	}
	return m[key]
}

type Map[K comparable, V any] struct {
	m map[K]V
}

func NewMap[K comparable, V any](capacity int) *Map[K, V] {
	return &Map[K, V]{
		m: make(map[K]V, capacity),
	}
}

func (m *Map[K, V]) AddOrUpdate(key K, value V) {
	if m.m == nil {
		m.m = make(map[K]V)
	}
	m.m[key] = value
}

func (m *Map[K, V]) AddIfNotExist(key K, value V) {
	if m.m == nil {
		m.m = make(map[K]V)
	}
	_, ok := m.m[key]
	if !ok {
		m.m[key] = value
	}
}

func (m *Map[K, V]) Exist(key K) bool {
	if m.m == nil {
		return false
	}
	_, ok := m.m[key]
	return ok
}

func (m *Map[K, V]) Length() int {
	if m.m == nil {
		return 0
	}
	return len(m.m)
}

func (m *Map[K, V]) Get(key K) (value V, ok bool) {
	if m.m == nil {
		ok = false
		return
	}
	v, ok := m.m[key]
	return v, ok
}

func (m *Map[K, V]) GetDefault(key K) (value V) {
	if m.m == nil {
		return
	}
	v := m.m[key]
	return v
}

func (m *Map[K, V]) Del(key K) {
	if m.m == nil {
		return
	}
	delete(m.m, key)
	return
}

func (m *Map[K, V]) Map() map[K]V {
	if m.m == nil {
		m.m = make(map[K]V)
	}
	return m.m
}
