package contracts

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"time"
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
	// Recoverer loads stored-but-undispatched messages at startup; the pipeline
	// dispatches them before any live traffic to preserve dispatch order.
	// Recover is retried with backoff until it succeeds or the pipeline context
	// is cancelled, and while it fails, dispatching is stalled and the pipeline
	// deliberately backs up: the Recoverer reads from the same datastore the
	// Writer writes to, so when recovery is impossible, durable writes are too,
	// and there is no live work worth admitting. Operators see RecoveryError
	// observations (wrapping monitoring.ErrRecovery) until the datastore is
	// restored, then RecoveryComplete; shutdown during the retry loop emits
	// RecoveryAbandoned and the next start retries recovery.
	Recoverer interface {
		Recover(context.Context) ([]*Message, error)
	}
	Serializer interface {
		Serialize(out io.Writer, in any) error
		ContentType() string
	}
	// Writer persists messages. The supplied messages are pooled and recycled
	// after Write returns; implementations must fully consume them before
	// returning and must not retain references to them or their Content.
	Writer interface {
		Write(ctx context.Context, messages ...*Message) error
	}
	// Dispatcher publishes messages. The supplied messages are pooled and
	// recycled after Dispatch returns; implementations must fully consume them
	// before returning and must not retain references to them or their Content.
	Dispatcher interface {
		Dispatch(ctx context.Context, messages ...*Message) error
	}
	Monitor interface {
		Track(observation any)
	}
	Waiter func(context.Context, time.Duration) error
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
