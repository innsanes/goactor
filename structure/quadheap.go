package structure

import (
	"cmp"
	"slices"
)

// HeapItem is an entry scheduled at When. Smaller deadlines are popped first.
type HeapItem[K comparable, V any, W cmp.Ordered] struct {
	Key   K
	Value V
	When  W
}

type heapEntry[K comparable, V any, W cmp.Ordered] struct {
	item     HeapItem[K, V, W]
	sequence uint64
}

// QuadHeap stores key-addressable entries ordered by deadline.
type QuadHeap[K comparable, V any, W cmp.Ordered] struct {
	items   []heapEntry[K, V, W]
	indexes map[K]int
	nextSeq uint64
}

// NewQuadHeap creates an empty heap with capacity reserved for capacity items.
func NewQuadHeap[K comparable, V any, W cmp.Ordered](capacity int) *QuadHeap[K, V, W] {
	if capacity < 0 {
		capacity = 0
	}
	return &QuadHeap[K, V, W]{
		items:   make([]heapEntry[K, V, W], 0, capacity),
		indexes: make(map[K]int, capacity),
	}
}

// Len returns the number of items in the heap.
func (h *QuadHeap[K, V, W]) Len() int {
	return len(h.items)
}

// Insert adds key, value, and when. It returns false when key is already present.
func (h *QuadHeap[K, V, W]) Insert(key K, value V, when W) bool {
	if _, ok := h.indexes[key]; ok {
		return false
	}
	if h.nextSeq == ^uint64(0) {
		h.renumberSequences()
	}
	h.items = append(h.items, heapEntry[K, V, W]{
		item:     HeapItem[K, V, W]{Key: key, Value: value, When: when},
		sequence: h.nextSeq,
	})
	h.nextSeq++
	h.indexes[key] = len(h.items) - 1
	h.siftUp(len(h.items) - 1)
	return true
}

// Peek returns the minimum item without removing it.
func (h *QuadHeap[K, V, W]) Peek() (HeapItem[K, V, W], bool) {
	if len(h.items) == 0 {
		return HeapItem[K, V, W]{}, false
	}
	return h.items[0].item, true
}

// Pop removes and returns the minimum item.
func (h *QuadHeap[K, V, W]) Pop() (HeapItem[K, V, W], bool) {
	if len(h.items) == 0 {
		return HeapItem[K, V, W]{}, false
	}
	return h.removeAt(0), true
}

// Update changes the value and deadline for key. It returns false when key is absent.
func (h *QuadHeap[K, V, W]) Update(key K, value V, when W) bool {
	i, ok := h.indexes[key]
	if !ok {
		return false
	}
	h.items[i].item.Value = value
	h.items[i].item.When = when
	h.fix(i)
	return true
}

// Upsert changes an existing item or inserts a new item. It returns true when
// it inserted a new item.
func (h *QuadHeap[K, V, W]) Upsert(key K, value V, when W) bool {
	if h.Update(key, value, when) {
		return false
	}
	h.Insert(key, value, when)
	return true
}

// Remove deletes key. It returns false when key is absent.
func (h *QuadHeap[K, V, W]) Remove(key K) bool {
	i, ok := h.indexes[key]
	if !ok {
		return false
	}
	h.removeAt(i)
	return true
}

func (h *QuadHeap[K, V, W]) less(i, j int) bool {
	left, right := h.items[i], h.items[j]
	return left.item.When < right.item.When || left.item.When == right.item.When && left.sequence < right.sequence
}

func (h *QuadHeap[K, V, W]) swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.indexes[h.items[i].item.Key] = i
	h.indexes[h.items[j].item.Key] = j
}

func (h *QuadHeap[K, V, W]) siftUp(i int) {
	for i > 0 {
		parent := (i - 1) / 4
		if !h.less(i, parent) {
			return
		}
		h.swap(i, parent)
		i = parent
	}
}

func (h *QuadHeap[K, V, W]) siftDown(i int) {
	for {
		least := i
		firstChild := 4*i + 1
		for child := firstChild; child < firstChild+4 && child < len(h.items); child++ {
			if h.less(child, least) {
				least = child
			}
		}
		if least == i {
			return
		}
		h.swap(i, least)
		i = least
	}
}

func (h *QuadHeap[K, V, W]) fix(i int) {
	if i > 0 && h.less(i, (i-1)/4) {
		h.siftUp(i)
		return
	}
	h.siftDown(i)
}

func (h *QuadHeap[K, V, W]) removeAt(i int) HeapItem[K, V, W] {
	item := h.items[i].item
	delete(h.indexes, item.Key)

	last := len(h.items) - 1
	if i == last {
		h.items = h.items[:last]
		if len(h.items) == 0 {
			h.nextSeq = 0
		}
		return item
	}

	h.items[i] = h.items[last]
	h.items = h.items[:last]
	h.indexes[h.items[i].item.Key] = i
	h.fix(i)
	return item
}

func (h *QuadHeap[K, V, W]) renumberSequences() {
	order := make([]int, len(h.items))
	for i := range h.items {
		order[i] = i
	}
	slices.SortFunc(order, func(left, right int) int {
		return cmp.Compare(h.items[left].sequence, h.items[right].sequence)
	})
	for sequence, index := range order {
		h.items[index].sequence = uint64(sequence)
	}
	h.nextSeq = uint64(len(h.items))
}
