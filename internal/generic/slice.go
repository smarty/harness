package generic

// Reclaim returns s truncated to a zero-length slice ready for reuse, clearing
// its elements so they become eligible for garbage collection. When s has grown
// beyond capacity, its oversized backing array is discarded and replaced with a
// fresh one of exactly capacity. This keeps a single pathological spike — e.g.
// one command broadcasting a huge burst of events — from pinning the oversized
// backing array in a sync.Pool or other long-lived buffer for the remaining
// life of the process, while steady-state reuse stays allocation-free (the
// elements clear and the array is retained) rather than re-growing from nil.
// Pass the buffer's normal working capacity as capacity.
func Reclaim[T any](s []T, capacity int) (result []T) {
	if cap(s) > capacity {
		return make([]T, 0, capacity)
	}
	clear(s)
	return s[:0]
}
