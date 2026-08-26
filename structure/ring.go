package structure

// Ring is a fixed-capacity FIFO ring buffer.
//
// Once the ring is full, Push overwrites the oldest value and returns it. The
// type is intentionally not synchronized; callers should provide
// synchronization when a ring is shared between goroutines.
type Ring[T any] struct {
	items []T
	head  int
	size  int
}

// NewRing creates a ring that can retain at most capacity values. A
// non-positive capacity creates a disabled ring that does not retain values.
func NewRing[T any](capacity int) *Ring[T] {
	if capacity < 0 {
		capacity = 0
	}
	return &Ring[T]{
		items: make([]T, capacity),
	}
}

// Len returns the number of values currently stored in the ring.
func (r *Ring[T]) Len() int {
	if r == nil {
		return 0
	}
	return r.size
}

// Cap returns the maximum number of values the ring can retain.
func (r *Ring[T]) Cap() int {
	if r == nil {
		return 0
	}
	return len(r.items)
}

// Full reports whether the ring is at capacity.
func (r *Ring[T]) Full() bool {
	return r != nil && r.size == len(r.items) && len(r.items) > 0
}

// Push appends value to the newest position. If the ring is full, it
// overwrites and returns the oldest value with overwritten=true. For a ring
// with zero capacity, value is discarded and overwritten is false.
func (r *Ring[T]) Push(value T) (evicted T, overwritten bool) {
	if r == nil || len(r.items) == 0 {
		return evicted, false
	}

	if r.size == len(r.items) {
		evicted = r.items[r.head]
		r.items[r.head] = value
		r.head = (r.head + 1) % len(r.items)
		return evicted, true
	}

	index := (r.head + r.size) % len(r.items)
	r.items[index] = value
	r.size++
	return evicted, false
}

// Peek returns the oldest value without removing it.
func (r *Ring[T]) Peek() (value T, ok bool) {
	if r == nil || r.size == 0 {
		return value, false
	}
	return r.items[r.head], true
}

// Pop removes and returns the oldest value.
func (r *Ring[T]) Pop() (value T, ok bool) {
	if r == nil || r.size == 0 {
		return value, false
	}

	value = r.items[r.head]
	var zero T
	r.items[r.head] = zero
	r.head = (r.head + 1) % len(r.items)
	r.size--
	if r.size == 0 {
		// Keep the empty ring in its canonical state. This also prevents the
		// next Push from depending on the old head position.
		r.head = 0
	}
	return value, true
}

// At returns the value at offset from the oldest value. At(0) is equivalent to
// Peek. It returns false when offset is outside the current ring contents.
func (r *Ring[T]) At(offset int) (value T, ok bool) {
	if r == nil || offset < 0 || offset >= r.size {
		return value, false
	}
	return r.items[(r.head+offset)%len(r.items)], true
}

// Values returns a copy of the current values in oldest-to-newest order.
func (r *Ring[T]) Values() []T {
	if r == nil || r.size == 0 {
		return nil
	}
	values := make([]T, r.size)
	for i := range values {
		values[i] = r.items[(r.head+i)%len(r.items)]
	}
	return values
}

// Clear removes all values while retaining the allocated storage.
func (r *Ring[T]) Clear() {
	if r == nil {
		return
	}
	var zero T
	for i := range r.items {
		r.items[i] = zero
	}
	r.head = 0
	r.size = 0
}
