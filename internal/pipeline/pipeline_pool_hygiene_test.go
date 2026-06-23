package pipeline

import (
	"context"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/better"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/internal/storage"
)

func TestPoolHygieneFixture(t *testing.T) {
	gunit.Run(new(PoolHygieneFixture), t)
}

// PoolHygieneFixture is a regression test for stale field values on pooled
// *contracts.Message instances: a message recycled by the terminal stage once
// carried a previous Type into later writes (wrong type names persisted to the
// database). It drives many messages of alternating value types through a real
// Build(...) pipeline so pooled messages are reused across different types,
// then asserts every message reaching the Writer carries the Type registered
// for its (current) Value.
//
// NOTE: the race detector deliberately randomizes sync.Pool reuse, so this
// test is at its most discriminating in runs without -race.
type PoolHygieneFixture struct {
	*gunit.Fixture

	writeLock sync.Mutex
	nextID    uint64
	written   []string
}

// Execute stands in for the contracts.Storage: it assigns ids on insert (so the
// Dispatcher's id!=0 guard is satisfied) and records each persisted message's Type.
func (this *PoolHygieneFixture) Exec(_ context.Context, operation any) error {
	if op, ok := operation.(*storage.InsertMessages); ok {
		this.writeLock.Lock()
		defer this.writeLock.Unlock()
		this.nextID = assignTestIDs(this.nextID, op.Messages)
		for _, message := range op.Messages {
			this.written = append(this.written, message.Type)
		}
	}
	return nil
}

func (this *PoolHygieneFixture) Track(any)                                             {}
func (this *PoolHygieneFixture) Serialize(io.Writer, any) error                        { return nil }
func (this *PoolHygieneFixture) ContentType() string                                   { return "" }
func (this *PoolHygieneFixture) Dispatch(context.Context, ...*contracts.Message) error { return nil }
func (this *PoolHygieneFixture) Decorate(_ context.Context, _ time.Time, message any) any {
	return message
}

func (this *PoolHygieneFixture) TestRecycledMessagesCarryTheTypeOfTheirCurrentValue() {
	ctx, cancel := context.WithCancel(this.Context())
	defer cancel()

	subject, err := Build(ctx, Configuration{
		Monitor:    this,
		Clock:      time.Now,
		Storage:    this,
		Serializer: this,
		Dispatcher: this,
		Decorator:  this,
		MessageTypes: map[reflect.Type]string{
			reflect.TypeOf(poolEventA{}): "pool-hygiene:event-a",
			reflect.TypeOf(poolEventB{}): "pool-hygiene:event-b",
		},
		DomainTypes:            []any{new(poolHygieneHandler)},
		BurstCapacity:          16,
		PipelineBufferCapacity: 1,
		ExecutionUnitSize:      1,
		ShedThreshold:          0.8,
	})
	this.So(err, better.BeNil)

	var waiter sync.WaitGroup
	for _, listener := range subject.Listeners {
		waiter.Go(listener.Listen)
	}

	// Sequential blocking calls maximize pool reuse; a long run of one type
	// followed by the other guarantees recycled messages cross type boundaries.
	const messagesPerType = 50
	for i := range messagesPerType * 2 {
		if i < messagesPerType {
			subject.BlockingEntrypoint.Handle(ctx, "A")
		} else {
			subject.BlockingEntrypoint.Handle(ctx, "B")
		}
	}
	this.So(subject.BlockingEntrypoint.(io.Closer).Close(), should.BeNil)
	waiter.Wait()

	this.writeLock.Lock()
	defer this.writeLock.Unlock()
	this.So(this.written, should.HaveLength, messagesPerType*2)
	for i, typeName := range this.written {
		expected := "pool-hygiene:event-a"
		if i >= messagesPerType {
			expected = "pool-hygiene:event-b"
		}
		if typeName != expected {
			this.Fatalf("message %d: got Type=%q, want %q (stale pooled Type?)", i, typeName, expected)
		}
	}
}

// TestRecycledBatchCarriesNoStaleContext guards against a recycled *batch
// pinning a prior request's context (and the request-scoped values it
// references) after the work it accompanied has completed.
func (this *PoolHygieneFixture) TestRecycledBatchCarriesNoStaleContext() {
	work := make(chan *batch, 1)
	subject := newEntrypoint(this, work, 0.80)

	ctx := context.WithValue(this.Context(), poolEventA{}, "request-scoped")
	go subject.Handle(ctx, "msg")

	item := <-work
	this.So(item.ctx, should.Equal, ctx)
	item.complete(true) // returns the batch to the pool

	recycled := subject.batches.Get()
	this.So(recycled.ctx, should.BeNil)
}

type (
	poolEventA struct{}
	poolEventB struct{}
)

type poolHygieneHandler struct{}

func (this *poolHygieneHandler) Execute(message any, broadcast func(...any)) {
	if typed, ok := message.(string); ok {
		this.ExecuteCommand(typed, broadcast)
	}
}
func (this *poolHygieneHandler) ExecuteCommand(message string, broadcast func(...any)) {
	if message == "A" {
		broadcast(poolEventA{})
	} else {
		broadcast(poolEventB{})
	}
}
