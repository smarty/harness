package pipeline

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/better"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/contracts/monitoring"
	"github.com/smarty/harness/v2/internal/storage"
)

func TestShutdownUnwedgeFixture(t *testing.T) {
	gunit.Run(new(ShutdownUnwedgeFixture), t)
}

// ShutdownUnwedgeFixture proves end-to-end through Build that Close() cannot
// deadlock behind a blocked sender: when the downstream is wedged (not erroring)
// and the work channel is full, Close() still returns promptly and the callers
// blocked enqueuing into BlockingEntrypoint.Handle panic with
// monitoring.ErrBatchAbandoned rather than deadlocking shutdown forever.
//
// The execution stage stops draining the entrypoint's work channel while a handler
// is blocked, so with BurstCapacity=1 the effective capacity is exactly 2 (one
// buffered, one held by the wedged execution); observing two BatchInFlight signals
// proves the channel is saturated and any further caller is blocked on the send.
type ShutdownUnwedgeFixture struct {
	*gunit.Fixture
	pipeline contracts.Pipeline
	waiter   sync.WaitGroup

	gate     chan struct{} // Execute blocks here until the test releases the wedge.
	inFlight chan struct{} // one signal per successfully-enqueued batch.
	nextID   uint64
}

type wedgeCommand string

func (this *ShutdownUnwedgeFixture) Setup() {
	this.gate = make(chan struct{})
	this.inFlight = make(chan struct{}, 16)

	var err error
	this.pipeline, err = Build(this.Context(), Configuration{
		Monitor:                this,
		Storage:                this,
		Serializer:             this,
		Dispatcher:             this,
		Decorator:              this,
		DomainTypes:            []any{this},
		BurstCapacity:          1, // tiny entrypoint channel: effective capacity 2 once wedged.
		PipelineBufferCapacity: 1,
		ExecutionUnitSize:      64,
		ShedThreshold:          0.8,
	})
	this.So(err, better.BeNil)

	for _, listener := range this.pipeline.Listeners {
		this.waiter.Go(listener.Listen)
	}
}

func (this *ShutdownUnwedgeFixture) ExecuteWedge(_ wedgeCommand, broadcast func(...any)) {
	<-this.gate // wedge the execution stage until the test releases it.
	broadcast("resulting-event")
}
func (this *ShutdownUnwedgeFixture) Execute(message any, broadcast func(...any)) {
	this.ExecuteWedge(message.(wedgeCommand), broadcast)
}
func (this *ShutdownUnwedgeFixture) Serialize(out io.Writer, _ any) error {
	_, _ = out.Write([]byte("encoded"))
	return nil
}
func (this *ShutdownUnwedgeFixture) ContentType() string { return "" }
func (this *ShutdownUnwedgeFixture) Dispatch(context.Context, ...*contracts.Message) error {
	return nil
}
func (this *ShutdownUnwedgeFixture) Decorate(ctx context.Context, message any) any {
	return message
}

// Execute stands in for the contracts.Storage: it assigns ids on insert so the two
// accepted batches dispatch cleanly once the wedge is released at shutdown.
func (this *ShutdownUnwedgeFixture) Exec(_ context.Context, operation any) error {
	if op, ok := operation.(*storage.InsertMessages); ok {
		this.nextID = assignTestIDs(this.nextID, op.Messages)
	}
	return nil
}

func (this *ShutdownUnwedgeFixture) Track(observation any) {
	if _, ok := observation.(monitoring.BatchInFlight); ok {
		this.inFlight <- struct{}{}
	}
}

func (this *ShutdownUnwedgeFixture) TestCloseUnwedgesBlockedHandleCallers() {
	const callers = 4
	panics := make(chan any, callers)
	for range callers {
		go func() {
			defer func() { panics <- recover() }()
			this.pipeline.BlockingEntrypoint.Handle(context.Background(), wedgeCommand("x"))
		}()
	}

	<-this.inFlight                   // one batch held by the wedged execution stage,
	<-this.inFlight                   // one batch buffered: the channel is now saturated.
	time.Sleep(20 * time.Millisecond) // let the remaining callers reach their blocked send.

	closed := make(chan error, 1)
	go func() { closed <- this.pipeline.BlockingEntrypoint.(io.Closer).Close() }()
	select {
	case err := <-closed:
		this.So(err, should.BeNil)
	case <-time.After(time.Second):
		this.Fatal("Close() deadlocked behind Handle callers blocked on a wedged pipeline")
	}

	close(this.gate) // release the wedge so the accepted batches drain and listeners exit.

	var abandoned int
	for range callers {
		if err, ok := (<-panics).(error); ok && errors.Is(err, monitoring.ErrBatchAbandoned) {
			abandoned++
		}
	}
	this.So(abandoned, should.Equal, callers-2) // exactly the two callers blocked on the send.

	this.waiter.Wait()
}
