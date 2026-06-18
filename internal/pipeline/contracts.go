package pipeline

import (
	"context"
	"time"

	"github.com/smarty/harness/v2/contracts"
)

// Internal interfaces — discovered reflectively from domain types
// supplied via Options.DomainTypes(...)
type (
	executor interface {
		Execute(message any, broadcast func(...any))
	}
	applicator interface {
		Apply(message any)
	}
)

// Internal collaborator interfaces — built from the caller-supplied
// Options.Storage(...) db at Build time, never accepted directly from callers.
type (
	// recoverer loads stored-but-undispatched messages at startup; the pipeline
	// dispatches them before any live traffic to preserve dispatch order.
	//
	// Recovery is paged: the pipeline calls Recover repeatedly, and each
	// successful call must return the next page — at most limit messages, in
	// dispatch order — of the backlog as it existed at startup, never messages
	// already returned by a prior successful call. An empty result means
	// recovery is complete: the pipeline stops calling and opens the gate to
	// live traffic. To bound resident memory, an implementation must therefore
	// snapshot the backlog's upper bound on its first call and page within it,
	// so rows written by live traffic during the recovery window are excluded
	// (they belong to the live path, not recovery).
	//
	// An error must not lose ground: the pipeline retries with backoff, and the
	// implementation must re-serve the failed page on the next call rather than
	// skip it (advance the cursor only after a page is returned cleanly).
	// Implementations are stateful cursors invoked from a single goroutine.
	//
	// Recover is retried with backoff until it succeeds or the pipeline context
	// is cancelled, and while it fails, dispatching is stalled and the pipeline
	// deliberately backs up: the recoverer reads from the same datastore the
	// writer writes to, so when recovery is impossible, durable writes are too,
	// and there is no live work worth admitting. Operators see RecoveryError
	// observations (wrapping monitoring.ErrRecovery) until the datastore is
	// restored, then RecoveryComplete; shutdown during the retry loop emits
	// RecoveryAbandoned and the next start retries recovery.
	recoverer interface {
		Recover(ctx context.Context, limit int) ([]*contracts.Message, error)
	}
	// writer persists messages. The supplied messages are pooled and recycled
	// after Write returns; implementations must fully consume them before
	// returning and must not retain references to them or their Content.
	writer interface {
		Write(ctx context.Context, messages ...*contracts.Message) error
	}
	// waiter pauses for the given duration during a retry backoff, returning
	// nil when the wait elapses or the context's error if it is cancelled
	// first. The persistence, broadcast, and recovery stages call it to stall
	// between failed attempts and to bail out promptly on shutdown; the wait
	// function in retry.go is the production implementation.
	waiter func(context.Context, time.Duration) error
)

// Unexported value types shared across pipeline stages.
type (
	batch struct {
		instructions []any
		complete     func(stored bool)
	}
	unitOfWork struct {
		results     []*contracts.Message
		completions []func(stored bool)
	}
)
