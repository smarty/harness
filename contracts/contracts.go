package contracts

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
)

// ErrInvalidConfiguration wraps every configuration-validation failure
// reported by New; test with errors.Is.
var ErrInvalidConfiguration = errors.New("harness: invalid configuration")

type Pipeline struct {
	// SheddingHTTPWrapper is meant to wrap around any http.Handler that calls SheddingEntrypoint.
	// It responds with HTTP 503 in the event that the handler is backed up beyond the configured ShedThreshold.
	SheddingHTTPWrapper func(http.Handler) http.Handler

	// SheddingEntrypoint is a Handler that is meant to be guarded by an admitter (such as SheddingHTTPWrapper).
	SheddingEntrypoint Handler

	// BlockingEntrypoint is a Handler that will block until the results of the provided work
	// have been durably stored: a normal return ALWAYS means the work was written.
	// If the pipeline context is cancelled before the work could ever be stored, Handle
	// panics with monitoring.ErrBatchAbandoned instead of returning — message brokers
	// acknowledge deliveries when Handle returns, and unstored work must never be
	// acknowledged. The panic ends the (already-shutting-down) process; the broker
	// redelivers, preserving the at-least-once contract.
	// Handle also panics with monitoring.ErrBatchAbandoned if the pipeline is closed
	// while the caller is still blocked enqueuing work into a wedged downstream: the
	// work was never stored, so the same no-false-acknowledgment rule applies.
	BlockingEntrypoint Handler

	// Listeners contains each phase of the harness pipeline (serialization, persistence, broadcast, etc.).
	// Each listener should be invoked on a separate goroutine by a component like github.com/smarty/dominoes.
	Listeners []Listener
}

// Interfaces common to many of our external and internal modules
type (
	Listener interface {
		Listen()
	}
	Handler interface {
		Handle(context.Context, ...any)
	}
)

// Collaborator interfaces — callers supply real implementations via Options.*
type (
	Serializer interface {
		Serialize(out io.Writer, in any) error
		ContentType() string
	}
	// Dispatcher publishes messages. The supplied messages are pooled and
	// recycled after Dispatch returns; implementations must fully consume them
	// before returning and must not retain references to them or their Content.
	Dispatcher interface {
		Dispatch(ctx context.Context, messages ...*Message) error
	}
	// Monitor receives observations emitted throughout the pipeline. Track is
	// invoked concurrently from many goroutines (the entrypoint, serialization,
	// persistence, broadcast, and recovery stages), so implementations must be
	// safe for concurrent use.
	Monitor interface {
		Track(observation any)
	}
)

// Message represents a record to be saved or loaded to/from the Messages database table.
// Pointers to this struct are often pooled and reused, so any consumer must NOT retain long-lived references.
type Message struct {
	// ID represents the unique ID of this message and its sequential place within a larger stream.
	ID uint64

	// Type is the name registered for the (Go) type of the Value.
	// (i.e. 'subscription:subscription-renewed-v2').
	Type string

	// Value contains the in-memory Go message structure.
	Value any

	// Content contains the serialized representation of the Go Value.
	Content *bytes.Buffer

	// ContentType identifies the serialization method employed to represent the Content
	// (i.e. 'application/json; charset=utf-8').
	ContentType string
}
