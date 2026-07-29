package structure

import (
	"math/rand"
	"strconv"
	"testing"
)

func TestQuadHeapInsertAndPop(t *testing.T) {
	h := NewQuadHeap(2)
	if !h.Insert("late", 30) || !h.Insert("early", 10) || !h.Insert("middle", 20) {
		t.Fatal("Insert() failed")
	}

	for _, want := range []HeapItem{
		{Key: "early", When: 10},
		{Key: "middle", When: 20},
		{Key: "late", When: 30},
	} {
		got, ok := h.Pop()
		if !ok || got != want {
			t.Fatalf("Pop() = %#v, %v; want %#v, true", got, ok, want)
		}
	}

	if _, ok := h.Pop(); ok {
		t.Fatal("empty Pop() returned ok")
	}
}

func TestQuadHeapKeyMutations(t *testing.T) {
	h := NewQuadHeap(-1)
	if !h.Insert("b", 20) || !h.Insert("a", 20) || h.Insert("a", 1) {
		t.Fatal("Insert() duplicate behavior is incorrect")
	}
	if !h.Update("b", 5) || h.Update("missing", 0) {
		t.Fatal("Update() behavior is incorrect")
	}
	if h.Upsert("b", 30) || !h.Upsert("c", 10) {
		t.Fatal("Upsert() result is incorrect")
	}
	if !h.Remove("c") || h.Remove("c") {
		t.Fatal("Remove() behavior is incorrect")
	}
	if got, ok := h.Pop(); !ok || got.Key != "a" || got.When != 20 {
		t.Fatalf("Pop() = %#v, %v; want Item{Key: \"a\", When: 20}, true", got, ok)
	}
}

func TestQuadHeapOrdersEqualDeadlinesByKey(t *testing.T) {
	h := NewQuadHeap(0)
	for _, key := range []string{"z", "a", "m"} {
		if !h.Insert(key, 1) {
			t.Fatalf("Insert(%q) failed", key)
		}
	}
	for _, want := range []string{"a", "m", "z"} {
		got, ok := h.Pop()
		if !ok || got.Key != want {
			t.Fatalf("Pop() = %#v, %v; want key %q", got, ok, want)
		}
	}
}

func TestQuadHeapRandomOperations(t *testing.T) {
	h := NewQuadHeap(4)
	reference := make(map[string]int64)
	rng := rand.New(rand.NewSource(42))

	for step := 0; step < 1000; step++ {
		key := "key-" + strconv.Itoa(rng.Intn(16))
		when := int64(rng.Intn(101) - 50)
		switch rng.Intn(5) {
		case 0:
			_, exists := reference[key]
			if got := h.Insert(key, when); got == exists {
				t.Fatalf("step %d: Insert(%q) = %v; exists = %v", step, key, got, exists)
			}
			if !exists {
				reference[key] = when
			}
		case 1:
			_, exists := reference[key]
			if got := h.Update(key, when); got != exists {
				t.Fatalf("step %d: Update(%q) = %v; exists = %v", step, key, got, exists)
			}
			if exists {
				reference[key] = when
			}
		case 2:
			_, exists := reference[key]
			if got := h.Upsert(key, when); got == exists {
				t.Fatalf("step %d: Upsert(%q) = %v; exists = %v", step, key, got, exists)
			}
			reference[key] = when
		case 3:
			_, exists := reference[key]
			if got := h.Remove(key); got != exists {
				t.Fatalf("step %d: Remove(%q) = %v; exists = %v", step, key, got, exists)
			}
			delete(reference, key)
		case 4:
			want, ok := referenceMinimum(reference)
			got, gotOK := h.Pop()
			if gotOK != ok || gotOK && got != want {
				t.Fatalf("step %d: Pop() = %#v, %v; want %#v, %v", step, got, gotOK, want, ok)
			}
			if ok {
				delete(reference, want.Key)
			}
		}
		assertInvariants(t, h, reference)
	}
}

func referenceMinimum(items map[string]int64) (HeapItem, bool) {
	var minimum HeapItem
	found := false
	for key, when := range items {
		item := HeapItem{Key: key, When: when}
		if !found || item.When < minimum.When || item.When == minimum.When && item.Key < minimum.Key {
			minimum, found = item, true
		}
	}
	return minimum, found
}

func assertInvariants(t *testing.T, h *QuadHeap, reference map[string]int64) {
	t.Helper()
	if h.Len() != len(reference) || len(h.indexes) != len(reference) {
		t.Fatalf("lengths: heap=%d indexes=%d reference=%d", h.Len(), len(h.indexes), len(reference))
	}
	for i, item := range h.items {
		if h.indexes[item.Key] != i || reference[item.Key] != item.When {
			t.Fatalf("item %d (%#v) is not synchronized", i, item)
		}
		for child := 4*i + 1; child < 4*i+5 && child < len(h.items); child++ {
			if h.less(child, i) {
				t.Fatalf("child %d (%#v) sorts before parent %d (%#v)", child, h.items[child], i, item)
			}
		}
	}
	if want, ok := referenceMinimum(reference); ok {
		got, gotOK := h.Peek()
		if !gotOK || got != want {
			t.Fatalf("Peek() = %#v, %v; want %#v, true", got, gotOK, want)
		}
	}
}
