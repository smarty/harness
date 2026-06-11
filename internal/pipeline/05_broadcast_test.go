package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/monitoring"
)

func TestBroadcastFixture(t *testing.T) {
	gunit.Run(new(BroadcastFixture), t)
}

type BroadcastFixture struct {
	*gunit.Fixture
	ctx     context.Context
	startup chan *unitOfWork
	input   chan *unitOfWork
	output  chan *unitOfWork
	waits   []time.Duration
	waitErr error
	subject *broadcast

	dispatchMu        sync.Mutex
	dispatchCalls     [][]*contracts.Message
	dispatchFailCount int

	tracked []any
}

func (this *BroadcastFixture) Setup() {
	this.ctx = context.WithValue(this.Context(), "testing", this.Name())
	this.startup = make(chan *unitOfWork, 1)
	close(this.startup) // most tests exercise steady-state input; startup tests rebuild the subject
	this.input = make(chan *unitOfWork, 4)
	this.output = make(chan *unitOfWork, 4)
	this.subject = newBroadcast(this.ctx, this, this.startup, this.input, this.output, this, this.wait)
}

func (this *BroadcastFixture) wait(_ context.Context, d time.Duration) error {
	this.waits = append(this.waits, d)
	return this.waitErr
}

func (this *BroadcastFixture) Track(observation any) {
	this.tracked = append(this.tracked, observation)
}

func (this *BroadcastFixture) Dispatch(ctx context.Context, messages ...*contracts.Message) error {
	this.So(ctx.Value("testing"), should.Equal, this.Name())
	captured := make([]*contracts.Message, len(messages))
	copy(captured, messages)
	this.dispatchMu.Lock()
	this.dispatchCalls = append(this.dispatchCalls, captured)
	this.dispatchMu.Unlock()
	if this.dispatchFailCount > 0 {
		this.dispatchFailCount--
		return errors.New("dispatch failure")
	}
	return nil
}

func (this *BroadcastFixture) drain() (results []*unitOfWork) {
	for unit := range this.output {
		results = append(results, unit)
	}
	return results
}

func (this *BroadcastFixture) TestDispatchesAllResultsThenForwardsUnit() {
	m1 := &contracts.Message{Value: "a"}
	m2 := &contracts.Message{Value: "b"}
	this.input <- &unitOfWork{results: []*contracts.Message{m1, m2}}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 1)
	this.So(len(this.dispatchCalls), should.Equal, 1)
	this.So(this.dispatchCalls[0], should.Equal, []*contracts.Message{m1, m2})
	this.So(this.waits, should.BeEmpty)
	this.So(this.tracked, should.BeEmpty)
}

func (this *BroadcastFixture) TestEachUnitDispatchedIndependently() {
	m1 := &contracts.Message{Value: "a"}
	m2 := &contracts.Message{Value: "b"}
	this.input <- &unitOfWork{results: []*contracts.Message{m1}}
	this.input <- &unitOfWork{results: []*contracts.Message{m2}}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 2)
	this.So(len(this.dispatchCalls), should.Equal, 2)
	this.So(this.dispatchCalls[0], should.Equal, []*contracts.Message{m1})
	this.So(this.dispatchCalls[1], should.Equal, []*contracts.Message{m2})
	this.So(this.tracked, should.BeEmpty)
}

func (this *BroadcastFixture) TestEmptyResultsTriggersEmptyDispatch() {
	this.input <- &unitOfWork{}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 1)
	this.So(len(this.dispatchCalls), should.Equal, 1)
	this.So(this.dispatchCalls[0], should.BeEmpty)
	this.So(this.tracked, should.BeEmpty)
}

func (this *BroadcastFixture) TestRetriesUntilDispatchSucceeds() {
	this.dispatchFailCount = 2
	m := &contracts.Message{Value: "retried"}
	this.input <- &unitOfWork{results: []*contracts.Message{m}}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 1)
	this.So(len(this.dispatchCalls), should.Equal, 3)
	this.So(this.waits, should.Equal, []time.Duration{time.Second, time.Second})
	this.So(this.tracked, should.HaveLength, 2)
	for n, observation := range this.tracked {
		failure, ok := observation.(monitoring.BroadcastError)
		this.So(ok, should.BeTrue)
		this.So(failure.Error, should.WrapError, monitoring.ErrBroadcast)
		this.So(failure.Attempt, should.Equal, n+1)
	}
}

func (this *BroadcastFixture) TestBroadcastAbandonsOnContextCancelButStillForwards() {
	this.dispatchFailCount = 1 << 30 // always fail
	this.waitErr = context.Canceled
	unit := &unitOfWork{results: []*contracts.Message{{Value: "abandoned"}}}
	this.input <- unit
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(units, should.Equal, []*unitOfWork{unit})
	this.So(len(this.dispatchCalls), should.Equal, 1)
	this.So(this.waits, should.Equal, []time.Duration{time.Second})
	this.So(this.tracked, should.HaveLength, 2)
	failure, ok := this.tracked[0].(monitoring.BroadcastError)
	this.So(ok, should.BeTrue)
	this.So(failure.Error, should.WrapError, monitoring.ErrBroadcast)
	this.So(failure.Attempt, should.Equal, 1)
	this.So(this.tracked[1], should.Equal, monitoring.BroadcastAbandoned{Attempts: 1})
}

func (this *BroadcastFixture) TestStartupUnitsDispatchedBeforeInputUnits() {
	this.startup = make(chan *unitOfWork, 1)
	this.subject = newBroadcast(this.ctx, this, this.startup, this.input, this.output, this, this.wait)
	recovered := &contracts.Message{Value: "recovered"}
	live := &contracts.Message{Value: "live"}
	this.input <- &unitOfWork{results: []*contracts.Message{live}}
	close(this.input)
	this.startup <- &unitOfWork{results: []*contracts.Message{recovered}}
	close(this.startup)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 2)
	this.So(len(this.dispatchCalls), should.Equal, 2)
	this.So(this.dispatchCalls[0], should.Equal, []*contracts.Message{recovered})
	this.So(this.dispatchCalls[1], should.Equal, []*contracts.Message{live})
	this.So(this.tracked, should.BeEmpty)
}

func (this *BroadcastFixture) TestClosedInputClosesOutput() {
	close(this.input)
	go this.subject.Listen()

	_, open := <-this.output
	this.So(open, should.BeFalse)
	this.So(this.tracked, should.BeEmpty)
}
