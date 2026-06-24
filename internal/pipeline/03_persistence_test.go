package pipeline

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/contracts/monitoring"
)

func TestPersistenceFixture(t *testing.T) {
	gunit.Run(new(PersistenceFixture), t)
}

type PersistenceFixture struct {
	*gunit.Fixture
	ctx     context.Context
	input   chan *unitOfWork
	output  chan *unitOfWork
	waits   []time.Duration
	waitErr error
	subject *persistence

	writeMu        sync.Mutex
	writeCalls     [][]*contracts.Message
	writeFailCount int

	tracked []any
}

func (this *PersistenceFixture) Setup() {
	this.ctx = context.WithValue(this.Context(), "testing", this.Name())
	this.input = make(chan *unitOfWork, 4)
	this.output = make(chan *unitOfWork, 4)
	this.subject = newPersistence(this.ctx, this, this.input, this.output, this, this.wait)
}

func (this *PersistenceFixture) wait(_ context.Context, d time.Duration) error {
	this.waits = append(this.waits, d)
	return this.waitErr
}

func (this *PersistenceFixture) Track(observation any) {
	this.tracked = append(this.tracked, observation)
}

func (this *PersistenceFixture) Write(ctx context.Context, messages ...*contracts.Message) error {
	this.So(ctx.Value("testing"), should.Equal, this.Name())
	this.writeMu.Lock()
	defer this.writeMu.Unlock()
	captured := make([]*contracts.Message, len(messages))
	copy(captured, messages)
	this.writeCalls = append(this.writeCalls, captured)
	if this.writeFailCount > 0 {
		this.writeFailCount--
		return errors.New("write failure")
	}
	return nil
}

func (this *PersistenceFixture) drain() (results []*unitOfWork) {
	return slices.Collect(Drain(this.output))
}

func (this *PersistenceFixture) TestWritesAllResultsThenForwardsUnit() {
	m1 := &contracts.Message{Value: "a"}
	m2 := &contracts.Message{Value: "b"}
	this.input <- &unitOfWork{results: []*contracts.Message{m1, m2}}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 1)
	this.So(len(this.writeCalls), should.Equal, 1)
	this.So(this.writeCalls[0], should.Equal, []*contracts.Message{m1, m2})
	this.So(this.waits, should.BeEmpty)
	this.So(this.tracked, should.Equal, []any{monitoring.ResultsPersisted{Count: 2}})
}

func (this *PersistenceFixture) TestEachUnitIsWrittenIndependently() {
	m1 := &contracts.Message{Value: "a"}
	m2 := &contracts.Message{Value: "b"}
	this.input <- &unitOfWork{results: []*contracts.Message{m1}}
	this.input <- &unitOfWork{results: []*contracts.Message{m2}}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 2)
	this.So(len(this.writeCalls), should.Equal, 2)
	this.So(this.writeCalls[0], should.Equal, []*contracts.Message{m1})
	this.So(this.writeCalls[1], should.Equal, []*contracts.Message{m2})
	this.So(this.tracked, should.Equal, []any{
		monitoring.ResultsPersisted{Count: 1},
		monitoring.ResultsPersisted{Count: 1},
	})
}

func (this *PersistenceFixture) TestRetriesUntilWriteSucceeds() {
	this.writeFailCount = 2
	m := &contracts.Message{Value: "retried"}
	this.input <- &unitOfWork{results: []*contracts.Message{m}}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 1)
	this.So(len(this.writeCalls), should.Equal, 3)
	assertBackoffWaits(this.Fixture, this.waits, 2)
	this.So(this.tracked, should.HaveLength, 3)
	for n, observation := range this.tracked[:2] {
		failure, ok := observation.(monitoring.PersistenceError)
		this.So(ok, should.BeTrue)
		this.So(failure.Error, should.WrapError, monitoring.ErrPersistence)
		this.So(failure.Attempt, should.Equal, n+1)
	}
	this.So(this.tracked[2], should.Equal, monitoring.ResultsPersisted{Count: 1})
}

func (this *PersistenceFixture) TestPersistenceAbandonsOnContextCancelAndDropsUnit() {
	this.writeFailCount = 1 << 30 // always fail
	this.waitErr = context.Canceled
	unit := &unitOfWork{results: []*contracts.Message{{Value: "abandoned"}}}
	this.input <- unit
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(units, should.BeEmpty)
	this.So(len(this.writeCalls), should.Equal, 1)
	assertBackoffWaits(this.Fixture, this.waits, 1)
	this.So(this.tracked, should.HaveLength, 2)
	failure, ok := this.tracked[0].(monitoring.PersistenceError)
	this.So(ok, should.BeTrue)
	this.So(failure.Error, should.WrapError, monitoring.ErrPersistence)
	this.So(failure.Attempt, should.Equal, 1)
	this.So(this.tracked[1], should.Equal, monitoring.PersistenceAbandoned{Attempts: 1})
}

func (this *PersistenceFixture) TestResultsPersistedIsCountedOnSuccess() {
	m1 := &contracts.Message{Value: "a"}
	m2 := &contracts.Message{Value: "b"}
	this.input <- &unitOfWork{results: []*contracts.Message{m1, m2}}
	close(this.input)

	go this.subject.Listen()

	this.drain()
	this.So(this.tracked, should.Equal, []any{monitoring.ResultsPersisted{Count: 2}})
}

func (this *PersistenceFixture) TestNothingPersistedIsCountedOnAbandonment() {
	this.writeFailCount = 1 << 30 // always fail
	this.waitErr = context.Canceled
	this.input <- &unitOfWork{results: []*contracts.Message{{Value: "abandoned"}}}
	close(this.input)

	go this.subject.Listen()

	this.drain()
	for _, observation := range this.tracked {
		_, isCount := observation.(monitoring.ResultsPersisted)
		this.So(isCount, should.BeFalse)
	}
}

func (this *PersistenceFixture) TestEmptyResultsSkipsWriteButForwards() {
	this.input <- &unitOfWork{}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 1)
	this.So(this.writeCalls, should.BeEmpty)
	for _, observation := range this.tracked {
		_, isCount := observation.(monitoring.ResultsPersisted)
		this.So(isCount, should.BeFalse)
	}
}

func (this *PersistenceFixture) TestClosedInputClosesOutput() {
	close(this.input)
	go this.subject.Listen()

	_, open := <-this.output
	this.So(open, should.BeFalse)
	this.So(this.tracked, should.BeEmpty)
}

func (this *PersistenceFixture) TestAbandonmentFailsCompletionsOfEveryDrainedUnit() {
	this.writeFailCount = 1 << 30 // always fail
	this.waitErr = context.Canceled
	var firstOutcomes, queuedOutcomes []bool
	this.input <- &unitOfWork{
		results:     []*contracts.Message{{Value: "abandoned"}},
		completions: []func(stored bool){func(stored bool) { firstOutcomes = append(firstOutcomes, stored) }},
	}
	this.input <- &unitOfWork{
		results:     []*contracts.Message{{Value: "queued-behind"}},
		completions: []func(stored bool){func(stored bool) { queuedOutcomes = append(queuedOutcomes, stored) }},
	}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(units, should.BeEmpty)
	this.So(firstOutcomes, should.Equal, []bool{false})
	this.So(queuedOutcomes, should.Equal, []bool{false})
}
