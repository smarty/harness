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

	// BlockingEntrypoint is a Handler that will block until the results of the provided work have been durably stored.
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
