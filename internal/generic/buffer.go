package generic

import "bytes"

// ReclaimBuffer is the *bytes.Buffer analog of Reclaim: it resets the buffer for
// reuse, but when the buffer's backing array has grown beyond capacity it is
// replaced with a fresh buffer of exactly capacity. This keeps a single
// oversized payload from pinning memory in a sync.Pool or other long-lived
// field for the remaining life of the process, while steady-state reuse simply
// resets and retains the existing array. Pass the buffer's normal working
// capacity as capacity.
func ReclaimBuffer(buffer *bytes.Buffer, capacity int) (result *bytes.Buffer) {
	if buffer.Cap() > capacity {
		return bytes.NewBuffer(make([]byte, 0, capacity))
	}
	buffer.Reset()
	return buffer
}
