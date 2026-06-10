// Package harness provides a staged, store-and-forward message-handling
// pipeline composed of goroutine stages (entrypoint, execution, serialization,
// persistence, completion, broadcast, terminal) connected by buffered channels.
//
// Callers register domain objects whose Execute.../Apply... methods drive the
// pipeline via Options.Types(...), and supply collaborators (Writer, Dispatcher,
// Serializer, Monitor) via the corresponding Options.*. All collaborators
// default to a no-op implementation, so omitting them produces a runnable but
// inert pipeline — useful for tests, but not for production.
//
// The only exported entry point is New(ctx, options...); every internal stage
// type is unexported and cannot be constructed directly by callers.
//
// The persistence and broadcast stages retry their collaborators on failure;
// those retry loops abort when the context passed to New(ctx, ...) is cancelled,
// so consumers must cancel it on shutdown to avoid hanging the drain. Custom
// Writer and Dispatcher implementations must honor the context they are given.
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

	"github.com/smarty/harness/v2/internal/contracts"
	"github.com/smarty/harness/v2/internal/pipeline"
)

// New constructs a staged, store-and-forward message-handling pipeline.
// Register domain types (handlers/observers) via Options.Types, and wire
// real Writer, Dispatcher, Serializer, and Monitor collaborators via the
// corresponding Options.* functions. Collaborators default to a shared
// no-op implementation, so omitting them produces a runnable but inert
// pipeline — useful for tests, but not for production.
func New(ctx context.Context, options ...option) contracts.Pipeline {
	var cfg pipeline.Configuration
	for _, apply := range Options.defaults(options...) {
		apply(&cfg)
	}
	return pipeline.Build(ctx, cfg)
}

var Options singleton

type singleton struct{}
type option func(*pipeline.Configuration)

// Types registers the domain objects whose Execute.../Apply... methods drive
// the pipeline. They are passed verbatim to newRouter(...) at build time.
func (singleton) Types(value ...any) option {
	return func(this *pipeline.Configuration) { this.Types = value }
}

// Monitor sets the Monitor collaborator that receives pipeline observations
// (BatchInFlight, BatchComplete, LoadShed, SerializationError, etc.).
func (singleton) Monitor(value contracts.Monitor) option {
	return func(this *pipeline.Configuration) { this.Monitor = value }
}

// Serializer sets the collaborator used to encode outgoing messages into bytes.
func (singleton) Serializer(value contracts.Serializer) option {
	return func(this *pipeline.Configuration) { this.Serializer = value }
}

// Writer sets the collaborator that persists encoded messages (e.g. to a database or message store).
func (singleton) Writer(value contracts.Writer) option {
	return func(this *pipeline.Configuration) { this.Writer = value }
}

// Dispatcher sets the collaborator that broadcasts outgoing messages to downstream consumers.
func (singleton) Dispatcher(value contracts.Dispatcher) option {
	return func(this *pipeline.Configuration) { this.Dispatcher = value }
}

// BurstCapacity sets the buffer size of the channel between the entrypoint and
// execution stages. Larger values absorb more burst traffic before back-pressure
// reaches callers. Default: 1024.
func (singleton) BurstCapacity(value int) option {
	return func(this *pipeline.Configuration) { this.BurstCapacity = value }
}

// PipelineBufferCapacity sets the buffer size of the channels connecting all pipeline
// stages after execution (serialization → persistence → completion → broadcast →
// terminal). Default: 4.
func (singleton) PipelineBufferCapacity(value int) option {
	return func(this *pipeline.Configuration) { this.PipelineBufferCapacity = value }
}

// ExecutionUnitSize sets the maximum number of batches coalesced into a single unit of
// work before the execution stage flushes downstream. Higher values increase
// throughput at the cost of latency per batch. Default: 64.
func (singleton) ExecutionUnitSize(value int) option {
	return func(this *pipeline.Configuration) { this.ExecutionUnitSize = value }
}

// SerializerCount sets the number of concurrent serialization goroutines.
// Default: 4.
func (singleton) SerializerCount(value int) option {
	return func(this *pipeline.Configuration) { this.SerializerCount = value }
}

// ShedThreshold sets the load-shedding threshold as a fraction of BurstCapacity
// in the range [0, 1]. When the batch channel fill ratio meets or exceeds this
// value, new callers are refused (admission returns 503; Handle is a no-op).
// This option only affects HTTP callers.
// Default: 0.80.
func (singleton) ShedThreshold(value float64) option {
	return func(this *pipeline.Configuration) { this.ShedThreshold = value }
}

func (singleton) defaults(options ...option) []option {
	blank := nop{}
	return append([]option{
		Options.Monitor(blank),
		Options.Serializer(blank),
		Options.Writer(blank),
		Options.Dispatcher(blank),
		Options.BurstCapacity(1024),
		Options.PipelineBufferCapacity(4),
		Options.ExecutionUnitSize(64),
		Options.SerializerCount(4),
		Options.ShedThreshold(0.80),
	}, options...)
}

// nop satisfies every collaborator interface so New(...) can be called with
// zero options and still produce a runnable (if inert) pipeline.
type nop struct{}

func (nop) Track(any)                                             {}
func (nop) Serialize(io.Writer, any) error                        { return nil }
func (nop) ContentType() string                                   { return "" }
func (nop) Write(context.Context, ...*contracts.Message) error    { return nil }
func (nop) Dispatch(context.Context, ...*contracts.Message) error { return nil }
