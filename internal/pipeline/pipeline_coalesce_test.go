package pipeline

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/better"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/contracts/monitoring"
	"github.com/smarty/harness/v2/internal/storage"
)

func TestCoalesceFixture(t *testing.T) {
	gunit.Run(new(CoalesceFixture), t)
}

const (
	coalesceCallers  = 256
	coalesceUnitSize = 64
)

// CoalesceFixture drives many concurrent BlockingEntrypoint.Handle callers through
// a real Build pipeline to exercise the execution stage's unit-coalescing gate
// (01_execution.go:67-69) under sustained load, which the strictly-sequential
// PipelineFixture never reached.
//
// Determinism without timing luck: the execution stage is pinned at a gate while
// every caller enqueues its batch, so the entire backlog is resident in the
// `batches` channel before a single unit is assembled. Releasing the gate then
// coalesces that backlog into the largest units the gate allows — exactly
// coalesceCallers/coalesceUnitSize units of coalesceUnitSize batches each — proving
// that under load the pipeline (a) loses and duplicates nothing and (b) genuinely
// merges independent callers into shared units rather than processing them
// one-at-a-time.
type CoalesceFixture struct {
	*gunit.Fixture
	pipeline contracts.Pipeline
	waiter   sync.WaitGroup

	gate     chan struct{} // Execute blocks here until the test releases the backlog.
	inFlight chan struct{} // one signal per successfully-enqueued caller.

	lock           sync.Mutex
	nextID         uint64
	written        []any
	dispatched     []any
	writeUnitSizes []int
}

type coalesceCommand int

func (this *CoalesceFixture) Setup() {
	this.gate = make(chan struct{})
	this.inFlight = make(chan struct{}, coalesceCallers)

	var err error
	this.pipeline, err = Build(this.Context(), Configuration{
		Monitor:                this,
		Storage:                this,
		Serializer:             this,
		Dispatcher:             this,
		Decorator:              this,
		DomainTypes:            []any{this},
		BurstCapacity:          coalesceCallers, // every caller enqueues without blocking.
		PipelineBufferCapacity: 4,
		ExecutionUnitSize:      coalesceUnitSize,
		ShedThreshold:          0.8,
	})
	this.So(err, better.BeNil)

	for _, listener := range this.pipeline.Listeners {
		this.waiter.Go(listener.Listen)
	}
}

// ExecuteCoalesce exists only so scan() registers coalesceCommand as a routed
// type; dispatch flows through the generic Execute below.
func (this *CoalesceFixture) ExecuteCoalesce(coalesceCommand, func(...any)) {}

func (this *CoalesceFixture) Execute(message any, broadcast func(...any)) {
	<-this.gate // hold the execution stage until every caller's batch is enqueued.
	broadcast(message)
}

func (this *CoalesceFixture) Serialize(out io.Writer, _ any) error {
	_, _ = out.Write([]byte("encoded"))
	return nil
}
func (this *CoalesceFixture) ContentType() string { return "" }

// Execute stands in for the contracts.Storage: it assigns ids on insert (so the
// Dispatcher's id!=0 guard is satisfied) and records each persisted unit's size.
func (this *CoalesceFixture) Exec(_ context.Context, operation any) error {
	if op, ok := operation.(*storage.InsertMessages); ok {
		this.lock.Lock()
		defer this.lock.Unlock()
		this.nextID = assignTestIDs(this.nextID, op.Messages)
		this.writeUnitSizes = append(this.writeUnitSizes, len(op.Messages))
		for _, message := range op.Messages {
			this.written = append(this.written, message.Value)
		}
	}
	return nil
}
func (this *CoalesceFixture) Dispatch(_ context.Context, messages ...*contracts.Message) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	for _, message := range messages {
		this.dispatched = append(this.dispatched, message.Value)
	}
	return nil
}
func (this *CoalesceFixture) Decorate(ctx context.Context, message any) any {
	return message
}

func (this *CoalesceFixture) Track(observation any) {
	if _, ok := observation.(monitoring.BatchInFlight); ok {
		this.inFlight <- struct{}{}
	}
}

func (this *CoalesceFixture) TestConcurrentCallersCoalesceUnderSustainedLoad() {
	var callers sync.WaitGroup
	for i := range coalesceCallers {
		command := coalesceCommand(i)
		callers.Add(1)
		go func() {
			defer callers.Done()
			this.pipeline.BlockingEntrypoint.Handle(context.Background(), command)
		}()
	}

	for range coalesceCallers {
		<-this.inFlight // every caller has enqueued; the full backlog is now resident.
	}
	close(this.gate) // release execution to coalesce the backlog into maximal units.

	callers.Wait() // every blocked Handle returns once its unit is stored and dispatched.
	this.shutdown()

	this.So(len(this.written), should.Equal, coalesceCallers)
	this.So(len(this.dispatched), should.Equal, coalesceCallers)
	this.So(this.distinctWritten(), should.Equal, coalesceCallers) // nothing lost or duplicated.
	this.So(this.writeUnitSizes, should.Equal, this.expectedUnitSizes())
}

func (this *CoalesceFixture) shutdown() {
	closer, ok := this.pipeline.BlockingEntrypoint.(io.Closer)
	this.So(ok, better.BeTrue)
	this.So(closer.Close(), should.BeNil)
	this.waiter.Wait()
}

func (this *CoalesceFixture) distinctWritten() int {
	this.lock.Lock()
	defer this.lock.Unlock()
	seen := make(map[any]struct{}, len(this.written))
	for _, value := range this.written {
		seen[value] = struct{}{}
	}
	return len(seen)
}

func (this *CoalesceFixture) expectedUnitSizes() (results []int) {
	for range coalesceCallers / coalesceUnitSize {
		results = append(results, coalesceUnitSize)
	}
	return results
}
