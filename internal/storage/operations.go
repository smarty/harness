package storage

import (
	"errors"

	"github.com/smarty/harness/v2/contracts"
)

var ErrUnsupportedOperation = errors.New("harness: unsupported storage operation")

type MarkMessagesDispatched struct {
	Messages []*contracts.Message
}

type InsertMessages struct {
	Messages []*contracts.Message
}

// LoadUndispatchedBounds reports the id range of the undispatched backlog.
type LoadUndispatchedBounds struct {
	Found bool   // false when the backlog is empty (both bounds NULL)
	Min   uint64 // populated by the handler
	Max   uint64
}

// LoadUndispatchedPage loads up to Limit undispatched messages in id order
// within the half-open window (AfterID, ThroughID].
type LoadUndispatchedPage struct {
	AfterID   uint64 // exclusive lower bound (the cursor)
	ThroughID uint64 // inclusive upper bound (the frozen boundary)
	Limit     int
	Messages  []*contracts.Message // populated by the handler
}
