package structure

type Set[T comparable] struct {
	m map[T]struct{}
}

func NewSet[T comparable](cap int) *Set[T] {
	return &Set[T]{
		m: make(map[T]struct{}, cap),
	}
}

func (s *Set[T]) Add(key T) {
	s.m[key] = struct{}{}
}

func (s *Set[T]) Del(key T) {
	delete(s.m, key)
}

func (s *Set[T]) Has(key T) bool {
	_, ok := s.m[key]
	return ok
}

func (s *Set[T]) Clear() {
	clear(s.m)
}
