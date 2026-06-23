// Package harness provides a staged, store-and-forward message-handling
// pipeline composed of goroutine stages (entrypoint, execution, serialization,
// persistence, completion, broadcast, terminal) connected by buffered channels.
// Each stage is a single goroutine, so units of work traverse the pipeline in
// execution order: messages are persisted (and therefore assigned ascending
// storage IDs) and dispatched in the order their units were executed.
//
// Callers register domain objects whose Execute.../Apply... methods drive the
// pipeline via Options.DomainTypes(...), and supply collaborators (Writer, Dispatcher,
// Serializer, Monitor, Decorator) via the corresponding Options.*. All collaborators
// default to a no-op implementation, so omitting them produces a runnable but
// inert pipeline — useful for tests, but not for production.
//
// The optional Decorator runs inside the execution stage, per-batch, over
// exactly the values a batch produced — before they are serialized and
// persisted — using the context that accompanied the originating command at the
// entrypoint. A replacement value must keep the same concrete Go type, since
// each message's registered Type name is derived before decoration runs.
//
// The only exported entry point is New(ctx, options...); every internal stage
// type is unexported and cannot be constructed directly by callers.
//
// The persistence and broadcast stages retry their collaborators on failure;
// those retry loops abort when the context passed to New(ctx, ...) is cancelled,
// so consumers must cancel it on shutdown to avoid hanging the drain. Custom
// Writer and Dispatcher implementations must honor the context they are given.
//
// Shutdown semantics for blocked callers: if persistence abandons a unit of
// work (context cancelled before the Writer ever succeeded), every caller
// blocked in BlockingEntrypoint.Handle (or awaiting via SheddingEntrypoint)
// for that unit panics with monitoring.ErrBatchAbandoned rather than
// returning. Returning normally would let message brokers acknowledge work
// that was never stored. The panic deliberately ends the process — it is
// already shutting down and can never make progress — and the broker
// redelivers after restart. The same panic releases a caller blocked
// enqueuing into a wedged downstream when the pipeline is closed: closing
// signals the entrypoint to abandon any in-flight send before it closes the
// work channel, so shutdown can never deadlock behind a stuck Handle even
// when the caller's context is never cancelled. Broadcast abandonment needs
// no such treatment: it occurs after the completion (ack) stage, and recovery
// redispatches stored-but-undispatched messages at next startup.
//
// Values produced by registered domain types must serialize successfully —
// that is the calling application's contract. If the Serializer ever returns
// an error, the pipeline tracks a SerializationError observation and then
// panics, halting the process before the unit of work reaches persistence:
// nothing is stored, acked, or dispatched, and the message source redelivers
// after restart. The failure is deterministic, so the application crash-loops
// until the offending domain type is fixed. Messages already in the pipeline
// may not persist (requiring retry from the caller or redelivery from the MQ).
package harness

import (
	"context"
	"io"
	"reflect"

	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/internal/pipeline"
	"github.com/smarty/harness/v2/internal/storage"
)

// New constructs a staged, store-and-forward message-handling pipeline.
// Register domain types (handlers/observers) via Options.DomainTypes, and wire
// real Writer, Dispatcher, Serializer, Monitor, and Decorator collaborators via
// the corresponding Options.* functions. Collaborators default to a shared
// no-op implementation, so omitting them produces a runnable but inert
// pipeline — useful for tests, but not for production.
//
// Exactly one of the returned values is non-nil (the zero Pipeline counting as
// nil): a non-nil error means the configuration is invalid (it wraps
// contracts.ErrInvalidConfiguration and names every offending value) and the
// application should not proceed.
func New(ctx context.Context, options ...option) (contracts.Pipeline, error) {
	var config pipeline.Configuration
	for _, apply := range Options.defaults(options...) {
		apply(&config)
	}
	return pipeline.Build(ctx, config)
}

var Options singleton

type singleton struct{}
type option func(*pipeline.Configuration)

// DomainTypes registers the domain objects whose Execute.../Apply... methods drive
// the pipeline. They are passed verbatim to newRouter(...) at build time.
//
// Each object must satisfy two parallel contracts: a typed discovery method per
// message (e.g. ExecuteRenewal(Renewal, func(...any)) or ApplyRenewal(Renewal))
// AND a generic dispatch method (Execute(any, func(...any)) / Apply(any)) that
// switches to the matching typed method. New rejects an object that has the
// typed methods but not the generic one, or whose typed method routes an
// interface type (which can never match a concrete runtime message). It cannot,
// however, detect a generic switch that omits a case it advertised via a typed
// method: such a message routes and then silently vanishes. Keep the switch and
// the typed methods in lockstep.
func (singleton) DomainTypes(value ...any) option {
	return func(this *pipeline.Configuration) { this.DomainTypes = value }
}

// MessageTypes allows the caller to specify the names of message types produced
// by the domain types for eventual persistence and publishing by the pipeline.
func (singleton) MessageTypes(value map[reflect.Type]string) option {
	return func(this *pipeline.Configuration) { this.MessageTypes = value }
}

// Monitor sets the Monitor collaborator that receives pipeline observations
// (BatchInFlight, BatchComplete, LoadShed, SerializationError, etc.).
func (singleton) Monitor(value contracts.Monitor) option {
	return func(this *pipeline.Configuration) { this.Monitor = value }
}

// Storage uses the provided db to build and set the Recovery, Writer,
// and Dispatcher components. Pass a concrete storage implementation such as
// storage/mysql.NewMapper(...); the seam itself is module-private, so the only
// supported implementation is the bundled MySQL mapper.
func (singleton) Storage(db storage.Storage) option {
	return func(this *pipeline.Configuration) { this.Storage = db }
}

// Serializer sets the collaborator used to encode outgoing messages into bytes.
func (singleton) Serializer(value contracts.Serializer) option {
	return func(this *pipeline.Configuration) { this.Serializer = value }
}

// Dispatcher sets the collaborator that broadcasts outgoing messages to downstream consumers.
func (singleton) Dispatcher(value contracts.Dispatcher) option {
	return func(this *pipeline.Configuration) { this.Dispatcher = value }
}

// Decorator sets the collaborator that transforms the values a domain handler
// produced — before they are serialized and persisted — using the context that
// accompanied the originating command. It runs per-batch within the execution
// stage. Defaults to a no-op that returns its messages unchanged.
func (singleton) Decorator(value contracts.Decorator) option {
	return func(this *pipeline.Configuration) { this.Decorator = value }
}

// BurstCapacity sets the buffer size of the channel between the entrypoint and
// execution stages. Larger values absorb more burst traffic before back-pressure
// reaches callers. Must be >= 1 or New returns an error. Default: 1024.
func (singleton) BurstCapacity(value int) option {
	return func(this *pipeline.Configuration) { this.BurstCapacity = value }
}

// PipelineBufferCapacity sets the buffer size of the channels connecting all pipeline
// stages after execution (serialization → persistence → completion → broadcast →
// terminal). Must be >= 0 (0 means unbuffered) or New returns an error. Default: 4.
func (singleton) PipelineBufferCapacity(value int) option {
	return func(this *pipeline.Configuration) { this.PipelineBufferCapacity = value }
}

// ExecutionUnitSize sets the maximum number of batches coalesced into a single unit of
// work before the execution stage flushes downstream. Higher values increase
// throughput at the cost of latency per batch. Must be >= 1 or New returns an
// error. Default: 64.
func (singleton) ExecutionUnitSize(value int) option {
	return func(this *pipeline.Configuration) { this.ExecutionUnitSize = value }
}

// ShedThreshold sets the load-shedding threshold as a fraction of BurstCapacity
// in the range [0, 1]. When the batch channel fill ratio meets or exceeds this
// value, new callers are refused (admission returns 503; Handle is a no-op).
// This option only affects HTTP callers.
// Values outside [0, 1] cause New to return an error. A value of 0 sheds all
// HTTP traffic — useful as a maintenance drain, but rarely what you want.
// Default: 0.80.
func (singleton) ShedThreshold(value float64) option {
	return func(this *pipeline.Configuration) { this.ShedThreshold = value }
}

func (singleton) defaults(options ...option) []option {
	var blank nop
	return append([]option{
		Options.Monitor(blank),
		Options.Storage(blank),
		Options.Serializer(blank),
		Options.Dispatcher(blank),
		Options.Decorator(blank),
		Options.BurstCapacity(1024),
		Options.PipelineBufferCapacity(4),
		Options.ExecutionUnitSize(64),
		Options.ShedThreshold(0.80),
	}, options...)
}

// nop satisfies every collaborator interface so New(...) can be called with
// zero options and still produce a runnable (if inert) pipeline.
type nop struct{}

func (nop) Track(any)                                             {}
func (nop) Serialize(io.Writer, any) error                        { return nil }
func (nop) ContentType() string                                   { return "" }
func (nop) Dispatch(context.Context, ...*contracts.Message) error { return nil }
func (nop) Exec(context.Context, any) error                       { return nil }
func (nop) Decorate(_ context.Context, messages []any) []any      { return messages }
