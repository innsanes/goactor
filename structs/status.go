package structs

import "slices"

type Status[T comparable] struct {
	status T
}

func NewStatus[T comparable](initStatus T) *Status[T] {
	return &Status[T]{
		status: initStatus,
	}
}

func (s *Status[T]) SetStatus(status T) {
	s.status = status
}

func (s *Status[T]) GetStatus() T {
	return s.status
}

func (s *Status[T]) IsStatus(status T) bool {
	return s.status == status
}

func (s *Status[T]) IsInStatus(status ...T) bool {
	if len(status) == 0 {
		return false
	}
	if !slices.Contains(status, s.status) {
		return false
	}
	return true
}

func (s *Status[T]) ChangeStatus(nextStatus T, prevStatus ...T) bool {
	if !s.IsInStatus(prevStatus...) {
		return false
	}
	s.status = nextStatus
	return true
}
