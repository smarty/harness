package pipeline

import (
	"context"
	"errors"
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

func TestAbandonmentFixture(t *testing.T) {
	gunit.Run(new(AbandonmentFixture), t)
}

// AbandonmentFixture proves the shutdown-abandonment contract end-to-end through Build: when the
// context is cancelled before the Writer ever succeeds, the goroutine blocked
// in BlockingEntrypoint.Handle panics (so message brokers never acknowledge
// unstored work) and nothing reaches the Dispatcher.
type AbandonmentFixture struct {
	*gunit.Fixture
	cancel   context.CancelFunc
	pipeline contracts.Pipeline
	waiter   sync.WaitGroup

	firstWrite    sync.Once
	writeAttempts chan struct{}

	dispatchLock  sync.Mutex
	dispatchCalls int
}

func (this *AbandonmentFixture) Decorate(ctx context.Context, messages []any) []any {
	return messages
}

type abandonedCommand string

func (this *AbandonmentFixture) Setup() {
	ctx, cancel := context.WithCancel(this.Context())
	this.cancel = cancel
	this.writeAttempts = make(chan struct{})

	var err error
	this.pipeline, err = Build(ctx, Configuration{
		Monitor:                this,
		Storage:                this,
		Serializer:             this,
		Dispatcher:             this,
		Decorator:              this,
		DomainTypes:            []any{this},
		BurstCapacity:          1024,
		PipelineBufferCapacity: 4,
		ExecutionUnitSize:      64,
		ShedThreshold:          0.8,
	})
	this.So(err, better.BeNil)

	for _, listener := range this.pipeline.Listeners {
		this.waiter.Go(listener.Listen)
	}
}

func (this *AbandonmentFixture) ExecuteCommand(_ abandonedCommand, broadcast func(...any)) {
	broadcast("resulting-event")
}
func (this *AbandonmentFixture) Execute(message any, broadcast func(...any)) {
	this.ExecuteCommand(message.(abandonedCommand), broadcast)
}
func (this *AbandonmentFixture) Serialize(out io.Writer, _ any) error {
	_, _ = out.Write([]byte("encoded"))
	return nil
}
func (this *AbandonmentFixture) ContentType() string { return "" }
func (this *AbandonmentFixture) Track(any)           {}

// Execute stands in for the contracts.Storage: the insert never succeeds, simulating a
// persistently unavailable database so the blocked Handle caller is abandoned.
func (this *AbandonmentFixture) Exec(_ context.Context, operation any) error {
	if _, ok := operation.(*storage.InsertMessages); ok {
		this.firstWrite.Do(func() { close(this.writeAttempts) })
		return errors.New("database unavailable")
	}
	return nil
}

func (this *AbandonmentFixture) Dispatch(context.Context, ...*contracts.Message) error {
	this.dispatchLock.Lock()
	defer this.dispatchLock.Unlock()
	this.dispatchCalls++
	return nil
}

func (this *AbandonmentFixture) TestBlockedHandleCallerPanicsWhenShutdownPrecedesDurableWrite() {
	panicked := make(chan any, 1)
	go func() {
		defer func() { panicked <- recover() }()
		this.pipeline.BlockingEntrypoint.Handle(context.Background(), abandonedCommand("doomed"))
	}()

	<-this.writeAttempts // the Writer is now failing; the caller is parked in Handle
	this.cancel()        // shutdown before any durable write ever succeeds

	recovered := <-panicked
	err, ok := recovered.(error)
	this.So(ok, should.BeTrue)
	this.So(err, should.WrapError, monitoring.ErrBatchAbandoned)

	this.So(this.pipeline.BlockingEntrypoint.(io.Closer).Close(), should.BeNil)
	this.waiter.Wait()

	this.dispatchLock.Lock()
	defer this.dispatchLock.Unlock()
	this.So(this.dispatchCalls, should.Equal, 0)
}
