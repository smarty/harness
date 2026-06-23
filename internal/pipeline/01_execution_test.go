package pipeline

import (
	"bytes"
	"context"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/better"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
)

func TestExecutionFixture(t *testing.T) {
	gunit.Run(new(ExecutionFixture), t)
}

type ExecutionFixture struct {
	*gunit.Fixture
	input     chan *batch
	output    chan *unitOfWork
	typeNames map[reflect.Type]string
	subject   *execution

	executeMu      sync.Mutex
	executeCalls   []any
	executeOutputs [][]any

	tracked []any
}

func (this *ExecutionFixture) getUnit() *unitOfWork {
	return new(unitOfWork)
}
func (this *ExecutionFixture) getMessage() *contracts.Message {
	return &contracts.Message{
		ID:          42,
		Type:        "stale type",
		Value:       []byte("stale value"),
		Content:     bytes.NewBufferString("stale content"),
		ContentType: "stale content type",
	}
}

func (this *ExecutionFixture) Setup() {
	this.input = make(chan *batch, 8)
	this.output = make(chan *unitOfWork, 8)
	this.typeNames = map[reflect.Type]string{
		reflect.TypeOf(""): "app:basic-string",
	}
	this.subject = newExecution(this, 64, this.getUnit, this.getMessage, this.typeNames, this.input, this.output, this, this)
}

func (this *ExecutionFixture) Execute(message any, broadcast func(...any)) {
	this.executeMu.Lock()
	defer this.executeMu.Unlock()
	this.executeCalls = append(this.executeCalls, message)
	if len(this.executeOutputs) == 0 {
		return
	}
	out := this.executeOutputs[0]
	this.executeOutputs = this.executeOutputs[1:]
	broadcast(out...)
}

func (this *ExecutionFixture) Track(observation any) {
	this.tracked = append(this.tracked, observation)
}

// Decorate is the fixture's default passthrough Decorator for tests that don't
// exercise decoration; tests that do supply their own (e.g. decoratorSpy).
func (this *ExecutionFixture) Decorate(_ context.Context, message any) any {
	return message
}

func (this *ExecutionFixture) drain() (results []*unitOfWork) {
	return slices.Collect(Drain(this.output))
}

func (this *ExecutionFixture) TestSingleBatchProducesUnitOfWork() {
	this.executeOutputs = [][]any{{"result-A"}}
	this.input <- &batch{instructions: []any{"msg-A"}, complete: func(bool) {}}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), better.Equal, 1)
	this.So(this.executeCalls, should.Equal, []any{"msg-A"})

	unit := units[0]
	this.So(len(unit.completions), better.Equal, 1)
	this.So(len(unit.results), better.Equal, 1)

	message := unit.results[0]
	this.So(message.ID, should.Equal, 0)
	this.So(message.Type, should.Equal, "app:basic-string")
	this.So(message.Value, should.Equal, "result-A")
	this.So(message.Content.String(), should.Equal, "")
	this.So(message.ContentType, should.Equal, "")
}

func (this *ExecutionFixture) TestUnitFlushesWhenMaxUnitSizeReached() {
	this.executeOutputs = [][]any{{"r1"}, {"r2"}, {"r3"}}
	this.input <- &batch{instructions: []any{"m1"}, complete: func(bool) {}}
	this.input <- &batch{instructions: []any{"m2"}, complete: func(bool) {}}
	this.input <- &batch{instructions: []any{"m3"}, complete: func(bool) {}}
	close(this.input)

	this.subject = newExecution(this, 2, this.getUnit, this.getMessage, this.typeNames, this.input, this.output, this, this)
	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 2)
	this.So(len(units[0].completions), should.Equal, 2)
	this.So(len(units[1].completions), should.Equal, 1)
	this.So(this.executeCalls, should.Equal, []any{"m1", "m2", "m3"})
}

// TestResidentBacklogCoalescesIntoSingleUnit and TestTrickledBatchesDoNotCoalesce
// are a contrasting pair pinning the load-dependent half of the coalescing gate
// (01_execution.go: `len(unit.completions) < maxUnitSize && len(this.input) > 0`).
// Identical batches, identical maxUnitSize: when the whole backlog is resident the
// stage merges it into one unit; when each batch is fully processed before the
// next arrives (input drains to empty) every batch flushes on its own. This is the
// unit-level proof underlying CoalesceFixture's end-to-end assertion — if the
// `len(this.input) > 0` term were dropped, the resident case would stop coalescing
// and this fixture would catch it.
func (this *ExecutionFixture) TestResidentBacklogCoalescesIntoSingleUnit() {
	this.executeOutputs = [][]any{{"r1"}, {"r2"}, {"r3"}}
	this.input <- &batch{instructions: []any{"m1"}, complete: func(bool) {}}
	this.input <- &batch{instructions: []any{"m2"}, complete: func(bool) {}}
	this.input <- &batch{instructions: []any{"m3"}, complete: func(bool) {}}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 1)
	this.So(len(units[0].completions), should.Equal, 3)
	this.So(this.executeCalls, should.Equal, []any{"m1", "m2", "m3"})
}

func (this *ExecutionFixture) TestTrickledBatchesDoNotCoalesce() {
	this.executeOutputs = [][]any{{"r1"}, {"r2"}, {"r3"}}
	go this.subject.Listen()

	var sizes []int
	for _, message := range []any{"m1", "m2", "m3"} {
		this.input <- &batch{instructions: []any{message}, complete: func(bool) {}}
		unit := <-this.output // block until this batch flushes before sending the next.
		sizes = append(sizes, len(unit.completions))
	}
	close(this.input)

	this.So(sizes, should.Equal, []int{1, 1, 1})
}

func (this *ExecutionFixture) TestEmptyExecutorOutputProducesUnitWithNoResults() {
	this.input <- &batch{instructions: []any{"silent"}, complete: func(bool) {}}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 1)
	this.So(units[0].results, should.BeEmpty)
	this.So(this.executeCalls, should.Equal, []any{"silent"})
}

func (this *ExecutionFixture) TestExecutorBroadcastsMultipleResults() {
	this.executeOutputs = [][]any{{"r1", "r2", "r3"}}
	this.input <- &batch{instructions: []any{"msg"}, complete: func(bool) {}}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 1)
	this.So(len(units[0].results), should.Equal, 3)
	this.So(units[0].results[0].Value, should.Equal, "r1")
	this.So(units[0].results[1].Value, should.Equal, "r2")
	this.So(units[0].results[2].Value, should.Equal, "r3")
}

func (this *ExecutionFixture) TestPerBatchDecorationUsesEachBatchContext() {
	spy := &decoratorSpy{}
	this.subject = newExecution(this, 64, this.getUnit, this.getMessage, this.typeNames, this.input, this.output, this, spy)

	ctxA := context.WithValue(this.Context(), spyKey{}, "A")
	ctxB := context.WithValue(this.Context(), spyKey{}, "B")

	this.executeOutputs = [][]any{{"a0", "a1"}, {"b0"}}
	this.input <- &batch{ctx: ctxA, instructions: []any{"cmdA"}, complete: func(bool) {}}
	this.input <- &batch{ctx: ctxB, instructions: []any{"cmdB"}, complete: func(bool) {}}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), better.Equal, 1) // both batches coalesce into one unit
	results := units[0].results
	this.So(len(results), better.Equal, 3)

	// (1) each value decorated with ITS OWN batch's ctx, (2) replacements wrote
	// back to Message.Value, (3) ordering preserved.
	this.So(results[0].Value, should.Equal, decoratedValue{ctx: ctxA, original: "a0"})
	this.So(results[1].Value, should.Equal, decoratedValue{ctx: ctxA, original: "a1"})
	this.So(results[2].Value, should.Equal, decoratedValue{ctx: ctxB, original: "b0"})

	// decoration ran once per batch, over exactly that batch's produced values.
	this.So(len(spy.calls), should.Equal, 3)
	this.So(spy.calls[0].ctx, should.Equal, ctxA)
	this.So(spy.calls[0].value, should.Equal, "a0")
	this.So(spy.calls[1].ctx, should.Equal, ctxA)
	this.So(spy.calls[1].value, should.Equal, "a1")
	this.So(spy.calls[2].ctx, should.Equal, ctxB)
	this.So(spy.calls[2].value, should.Equal, "b0")
}

func (this *ExecutionFixture) TestClosedInputClosesOutput() {
	close(this.input)
	go this.subject.Listen()

	_, open := <-this.output
	this.So(open, should.BeFalse)
	this.So(this.tracked, should.BeEmpty)
}

type spyKey struct{}

// decoratedValue is the replacement value the spy writes back, capturing which
// ctx decorated which original value so the test can assert per-batch wiring.
type decoratedValue struct {
	ctx      context.Context
	original any
}

type decoratorSpy struct {
	calls []spyCall
}
type spyCall struct {
	ctx   context.Context
	value any
}

func (this *decoratorSpy) Decorate(ctx context.Context, message any) any {
	this.calls = append(this.calls, spyCall{ctx: ctx, value: message})
	return decoratedValue{ctx: ctx, original: message}
}
