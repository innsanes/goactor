package structure

import (
	"math"
	"math/rand"
	"strconv"
	"testing"
)

type payload struct{ name string }

func TestQuadHeapGenericInsertAndPop(t *testing.T) {
	h := NewQuadHeap[string, payload, int64](2)
	if !h.Insert("late", payload{"L"}, 30) || !h.Insert("early", payload{"E"}, 10) || !h.Insert("middle", payload{"M"}, 20) {
		t.Fatal("Insert() failed")
	}
	for _, want := range []HeapItem[string, payload, int64]{
		{Key: "early", Value: payload{"E"}, When: 10},
		{Key: "middle", Value: payload{"M"}, When: 20},
		{Key: "late", Value: payload{"L"}, When: 30},
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

func TestQuadHeapGenericKeyMutations(t *testing.T) {
	h := NewQuadHeap[int, string, int](0)
	if !h.Insert(2, "two", 20) || !h.Insert(1, "one", 20) || h.Insert(1, "duplicate", 1) {
		t.Fatal("Insert() duplicate behavior is incorrect")
	}
	if !h.Update(2, "updated", 5) || h.Update(3, "missing", 0) {
		t.Fatal("Update() behavior is incorrect")
	}
	if h.Upsert(2, "later", 30) || !h.Upsert(3, "three", 10) {
		t.Fatal("Upsert() result is incorrect")
	}
	if !h.Remove(3) || h.Remove(3) {
		t.Fatal("Remove() behavior is incorrect")
	}
	if got, ok := h.Pop(); !ok || got != (HeapItem[int, string, int]{Key: 1, Value: "one", When: 20}) {
		t.Fatalf("Pop() = %#v, %v", got, ok)
	}
}

func TestQuadHeapUsesFIFOForEqualDeadlines(t *testing.T) {
	h := NewQuadHeap[string, int, int64](0)
	for i, key := range []string{"z", "a", "m"} {
		if !h.Insert(key, i, 1) {
			t.Fatalf("Insert(%q) failed", key)
		}
	}
	for _, want := range []string{"z", "a", "m"} {
		got, ok := h.Pop()
		if !ok || got.Key != want {
			t.Fatalf("Pop() = %#v, %v; want key %q", got, ok, want)
		}
	}
}

func TestQuadHeapUpdateRetainsFIFOSequence(t *testing.T) {
	h := NewQuadHeap[string, int, int64](0)
	h.Insert("first", 1, 10)
	h.Insert("second", 2, 20)
	if !h.Update("second", 20, 10) {
		t.Fatal("Update() failed")
	}
	for _, want := range []string{"first", "second"} {
		got, ok := h.Pop()
		if !ok || got.Key != want {
			t.Fatalf("Pop() = %#v, %v; want key %q", got, ok, want)
		}
	}
}

func TestQuadHeapSupportsStringDeadlines(t *testing.T) {
	h := NewQuadHeap[int, string, string](0)
	h.Insert(1, "late", "2026-07-30")
	h.Insert(2, "early", "2026-07-29")
	got, ok := h.Pop()
	if !ok || got != (HeapItem[int, string, string]{Key: 2, Value: "early", When: "2026-07-29"}) {
		t.Fatalf("Pop() = %#v, %v", got, ok)
	}
}

func TestQuadHeapResetsSequenceWhenEmpty(t *testing.T) {
	h := NewQuadHeap[string, int, int64](0)
	h.Insert("only", 1, 1)
	h.Pop()
	if h.nextSeq != 0 {
		t.Fatalf("nextSeq = %d; want 0 after heap becomes empty", h.nextSeq)
	}
}

func TestQuadHeapRenumbersBeforeSequenceWrap(t *testing.T) {
	h := NewQuadHeap[string, int, int64](0)
	h.Insert("first", 1, 1)
	h.Insert("second", 2, 1)
	h.nextSeq = math.MaxUint64 - 1
	h.Insert("third", 3, 1)
	h.Insert("fourth", 4, 1)
	h.Insert("fifth", 5, 1)

	for _, want := range []string{"first", "second", "third", "fourth", "fifth"} {
		got, ok := h.Pop()
		if !ok || got.Key != want {
			t.Fatalf("Pop() = %#v, %v; want key %q", got, ok, want)
		}
	}
}

type referenceEntry struct {
	value    int
	when     int64
	sequence uint64
}

func TestQuadHeapRandomOperations(t *testing.T) {
	h := NewQuadHeap[string, int, int64](4)
	reference := make(map[string]referenceEntry)
	rng := rand.New(rand.NewSource(42))
	var nextSequence uint64

	for step := range 1000 {
		key := "key-" + strconv.Itoa(rng.Intn(16))
		value := rng.Intn(1000)
		when := int64(rng.Intn(101) - 50)
		switch rng.Intn(5) {
		case 0:
			_, exists := reference[key]
			if got := h.Insert(key, value, when); got == exists {
				t.Fatalf("step %d: Insert(%q) = %v; exists = %v", step, key, got, exists)
			}
			if !exists {
				reference[key] = referenceEntry{value: value, when: when, sequence: nextSequence}
				nextSequence++
			}
		case 1:
			entry, exists := reference[key]
			if got := h.Update(key, value, when); got != exists {
				t.Fatalf("step %d: Update(%q) = %v; exists = %v", step, key, got, exists)
			}
			if exists {
				entry.value, entry.when = value, when
				reference[key] = entry
			}
		case 2:
			entry, exists := reference[key]
			if got := h.Upsert(key, value, when); got == exists {
				t.Fatalf("step %d: Upsert(%q) = %v; exists = %v", step, key, got, exists)
			}
			if exists {
				entry.value, entry.when = value, when
			} else {
				entry = referenceEntry{value: value, when: when, sequence: nextSequence}
				nextSequence++
			}
			reference[key] = entry
		case 3:
			_, exists := reference[key]
			if got := h.Remove(key); got != exists {
				t.Fatalf("step %d: Remove(%q) = %v; exists = %v", step, key, got, exists)
			}
			delete(reference, key)
		case 4:
			want, wantEntry, ok := referenceMinimum(reference)
			got, gotOK := h.Pop()
			if gotOK != ok || gotOK && (got.Key != want || got.Value != wantEntry.value || got.When != wantEntry.when) {
				t.Fatalf("step %d: Pop() = %#v, %v; want key %q, %v", step, got, gotOK, want, ok)
			}
			if ok {
				delete(reference, want)
			}
		}
		if len(reference) == 0 {
			nextSequence = 0
		}
		assertInvariants(t, h, reference)
	}
}

func referenceMinimum(items map[string]referenceEntry) (string, referenceEntry, bool) {
	var minimumKey string
	var minimum referenceEntry
	found := false
	for key, entry := range items {
		if !found || entry.when < minimum.when || entry.when == minimum.when && entry.sequence < minimum.sequence {
			minimumKey, minimum, found = key, entry, true
		}
	}
	return minimumKey, minimum, found
}

func assertInvariants(t *testing.T, h *QuadHeap[string, int, int64], reference map[string]referenceEntry) {
	t.Helper()
	if h.Len() != len(reference) || len(h.indexes) != len(reference) {
		t.Fatalf("lengths: heap=%d indexes=%d reference=%d", h.Len(), len(h.indexes), len(reference))
	}
	for i, entry := range h.items {
		referenceEntry, ok := reference[entry.item.Key]
		if !ok || h.indexes[entry.item.Key] != i || referenceEntry.value != entry.item.Value || referenceEntry.when != entry.item.When || referenceEntry.sequence != entry.sequence {
			t.Fatalf("entry %d (%#v) is not synchronized", i, entry)
		}
		for child := 4*i + 1; child < 4*i+5 && child < len(h.items); child++ {
			if h.less(child, i) {
				t.Fatalf("child %d (%#v) sorts before parent %d (%#v)", child, h.items[child], i, entry)
			}
		}
	}
}
