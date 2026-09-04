package fec

import "time"

type expiryEntry struct {
	key      BlockKey
	deadline time.Time
	index    int
}

type expiryHeap []*expiryEntry

func (h expiryHeap) Len() int { return len(h) }

func (h expiryHeap) Less(i, j int) bool {
	if !h[i].deadline.Equal(h[j].deadline) {
		return h[i].deadline.Before(h[j].deadline)
	}
	return keyLess(h[i].key, h[j].key)
}

func (h expiryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *expiryHeap) Push(value any) {
	entry := value.(*expiryEntry)
	entry.index = len(*h)
	*h = append(*h, entry)
}

func (h *expiryHeap) Pop() any {
	old := *h
	last := len(old) - 1
	entry := old[last]
	old[last] = nil
	entry.index = -1
	*h = old[:last]
	return entry
}

func keyLess(a, b BlockKey) bool {
	for index := range a.SessionID {
		if a.SessionID[index] != b.SessionID[index] {
			return a.SessionID[index] < b.SessionID[index]
		}
	}
	return a.PacketID < b.PacketID
}
