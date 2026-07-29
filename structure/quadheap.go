package structure

// HeapItem is an entry scheduled at When. Smaller deadlines are popped first.
type HeapItem struct {
	Key  string
	When int64
}

// QuadHeap stores timer entries ordered by deadline.
type QuadHeap struct {
	items   []HeapItem
	indexes map[string]int
}

// NewQuadHeap creates an empty heap with capacity reserved for capacity items.
func NewQuadHeap(capacity int) *QuadHeap {
	if capacity < 0 {
		capacity = 0
	}
	return &QuadHeap{
		items:   make([]HeapItem, 0, capacity),
		indexes: make(map[string]int, capacity),
	}
}

// Len returns the number of items in the heap.
func (h *QuadHeap) Len() int {
	return len(h.items)
}

// Insert adds key at when. It returns false when key is already present.
func (h *QuadHeap) Insert(key string, when int64) bool {
	if _, ok := h.indexes[key]; ok {
		return false
	}

	h.items = append(h.items, HeapItem{Key: key, When: when})
	h.indexes[key] = len(h.items) - 1
	h.siftUp(len(h.items) - 1)
	return true
}

// Peek returns the minimum item without removing it.
func (h *QuadHeap) Peek() (HeapItem, bool) {
	if len(h.items) == 0 {
		return HeapItem{}, false
	}
	return h.items[0], true
}

// Pop removes and returns the minimum item.
func (h *QuadHeap) Pop() (HeapItem, bool) {
	if len(h.items) == 0 {
		return HeapItem{}, false
	}
	return h.removeAt(0), true
}

// Update changes the deadline for key. It returns false when key is absent.
func (h *QuadHeap) Update(key string, when int64) bool {
	i, ok := h.indexes[key]
	if !ok {
		return false
	}
	h.items[i].When = when
	h.fix(i)
	return true
}

// Upsert changes an existing deadline or inserts a new item. It returns true
// when it inserted a new item.
func (h *QuadHeap) Upsert(key string, when int64) bool {
	if h.Update(key, when) {
		return false
	}
	h.Insert(key, when)
	return true
}

// Remove deletes key. It returns false when key is absent.
func (h *QuadHeap) Remove(key string) bool {
	i, ok := h.indexes[key]
	if !ok {
		return false
	}
	h.removeAt(i)
	return true
}

func (h *QuadHeap) less(i, j int) bool {
	left, right := h.items[i], h.items[j]
	return left.When < right.When || left.When == right.When && left.Key < right.Key
}

func (h *QuadHeap) swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.indexes[h.items[i].Key] = i
	h.indexes[h.items[j].Key] = j
}

func (h *QuadHeap) siftUp(i int) {
	for i > 0 {
		parent := (i - 1) / 4
		if !h.less(i, parent) {
			return
		}
		h.swap(i, parent)
		i = parent
	}
}

func (h *QuadHeap) siftDown(i int) {
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

func (h *QuadHeap) fix(i int) {
	if i > 0 && h.less(i, (i-1)/4) {
		h.siftUp(i)
		return
	}
	h.siftDown(i)
}

func (h *QuadHeap) removeAt(i int) HeapItem {
	item := h.items[i]
	delete(h.indexes, item.Key)

	last := len(h.items) - 1
	if i == last {
		h.items = h.items[:last]
		return item
	}

	h.items[i] = h.items[last]
	h.items = h.items[:last]
	h.indexes[h.items[i].Key] = i
	h.fix(i)
	return item
}
