package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/contracts/monitoring"
)

func TestRecoveryFixture(t *testing.T) {
	gunit.Run(new(RecoveryFixture), t)
}

type RecoveryFixture struct {
	*gunit.Fixture
	ctx       context.Context
	output    chan *unitOfWork
	batchSize int
	waits     []time.Duration
	waitErr   error
	tracked   []any
	subject   *Recovery

	// script drives Recover: each entry is either a page ([]*contracts.Message)
	// or an error. Recover pops the next entry per call, returning an empty page
	// once the script is exhausted (recovery complete).
	script       []any
	recoverCalls int
	units        []int // sizes of the units forwarded on output, captured while draining
}

func (this *RecoveryFixture) Setup() {
	this.ctx = context.WithValue(this.Context(), "testing", this.Name())
	this.output = make(chan *unitOfWork, 4)
	this.batchSize = 1
	this.subject = newRecovery(this.ctx, this, this.batchSize, this.output, this.wait, this)
}

func (this *RecoveryFixture) wait(_ context.Context, d time.Duration) error {
	this.waits = append(this.waits, d)
	return this.waitErr
}

func (this *RecoveryFixture) Track(observation any) {
	this.tracked = append(this.tracked, observation)
}

func (this *RecoveryFixture) Recover(ctx context.Context, limit int) ([]*contracts.Message, error) {
	this.So(ctx.Value("testing"), should.Equal, this.Name())
	this.So(limit, should.Equal, this.batchSize)
	this.recoverCalls++
	if len(this.script) == 0 {
		return nil, nil
	}
	next := this.script[0]
	this.script = this.script[1:]
	if err, ok := next.(error); ok {
		return nil, err
	}
	return next.([]*contracts.Message), nil
}

func (this *RecoveryFixture) drain() (results []*contracts.Message) {
	for unit := range this.output {
		this.units = append(this.units, len(unit.results))
		results = append(results, unit.results...)
	}
	return results
}

func (this *RecoveryFixture) TestNothingToRecover_EmitsNoUnitAndClosesOutput() {
	go this.subject.Listen()
	results := this.drain()

	this.So(results, should.BeEmpty)
	this.So(this.recoverCalls, should.Equal, 1)
	this.So(this.waits, should.BeEmpty)
	this.So(this.tracked, should.Equal, []any{monitoring.RecoveryComplete{Count: 0}})
}

func (this *RecoveryFixture) TestRecoveredMessages_EmittedThenOutputClosed() {
	page := []*contracts.Message{{ID: 1}, {ID: 2}}
	this.script = []any{page}

	go this.subject.Listen()
	results := this.drain()

	this.So(results, should.Equal, page)
	this.So(this.recoverCalls, should.Equal, 2) // the page, then the empty page that completes recovery
	this.So(this.waits, should.BeEmpty)
	this.So(this.tracked, should.Equal, []any{monitoring.RecoveryComplete{Count: 2}})
}

func (this *RecoveryFixture) TestRecoverError_TracksThenWaitsThenRetries() {
	boom := errors.New("boom")
	page := []*contracts.Message{{ID: 1}}
	this.script = []any{boom, boom, page}

	go this.subject.Listen()
	results := this.drain()

	this.So(results, should.Equal, page)
	this.So(this.recoverCalls, should.Equal, 4) // two failures, the page, then the empty page
	assertBackoffWaits(this.Fixture, this.waits, 2)
	this.So(this.tracked, should.HaveLength, 3)
	for n, observation := range this.tracked[:2] {
		failure, ok := observation.(monitoring.RecoveryError)
		this.So(ok, should.BeTrue)
		this.So(failure.Error, should.WrapError, monitoring.ErrRecovery)
		this.So(failure.Error, should.WrapError, boom)
		this.So(failure.Attempt, should.Equal, n+1)
	}
	this.So(this.tracked[2], should.Equal, monitoring.RecoveryComplete{Count: 1})
}

func (this *RecoveryFixture) TestRecoverError_WaitFails_AbandonsAndClosesOutput() {
	boom := errors.New("boom")
	this.script = []any{boom}
	this.waitErr = context.Canceled

	go this.subject.Listen()
	results := this.drain()

	this.So(results, should.BeEmpty)
	this.So(this.recoverCalls, should.Equal, 1)
	assertBackoffWaits(this.Fixture, this.waits, 1)
	this.So(this.tracked, should.HaveLength, 2)
	failure, ok := this.tracked[0].(monitoring.RecoveryError)
	this.So(ok, should.BeTrue)
	this.So(failure.Error, should.WrapError, monitoring.ErrRecovery)
	this.So(failure.Error, should.WrapError, boom)
	this.So(failure.Attempt, should.Equal, 1)
	this.So(this.tracked[1], should.Equal, monitoring.RecoveryAbandoned{Attempts: 1})
}

func (this *RecoveryFixture) TestMultiplePages_ForwardedInOrder_SingleCompleteTotal() {
	this.script = []any{
		[]*contracts.Message{{ID: 1}, {ID: 2}},
		[]*contracts.Message{{ID: 3}},
	}

	go this.subject.Listen()
	results := this.drain()

	this.So(this.recoverCalls, should.Equal, 3) // two pages, then the empty page
	this.So(this.units, should.Equal, []int{1, 1, 1})
	this.So(len(results), should.Equal, 3)
	this.So(results[0].ID, should.Equal, uint64(1))
	this.So(results[1].ID, should.Equal, uint64(2))
	this.So(results[2].ID, should.Equal, uint64(3))
	this.So(this.waits, should.BeEmpty)
	this.So(this.tracked, should.Equal, []any{monitoring.RecoveryComplete{Count: 3}})
}

func (this *RecoveryFixture) TestErrorBetweenPages_ResumesAndResetsAttempt() {
	boom := errors.New("boom")
	this.script = []any{
		[]*contracts.Message{{ID: 1}},
		boom,
		[]*contracts.Message{{ID: 2}},
	}

	go this.subject.Listen()
	results := this.drain()

	this.So(this.recoverCalls, should.Equal, 4) // page, failure, page, empty page
	this.So(len(results), should.Equal, 2)
	this.So(results[0].ID, should.Equal, uint64(1))
	this.So(results[1].ID, should.Equal, uint64(2))
	assertBackoffWaits(this.Fixture, this.waits, 1)
	this.So(this.tracked, should.HaveLength, 2)
	failure, ok := this.tracked[0].(monitoring.RecoveryError)
	this.So(ok, should.BeTrue)
	this.So(failure.Attempt, should.Equal, 1) // streak reset by the first page's success
	this.So(this.tracked[1], should.Equal, monitoring.RecoveryComplete{Count: 2})
}

func (this *RecoveryFixture) TestAbandonmentMidPagination_NoComplete() {
	boom := errors.New("boom")
	this.script = []any{
		[]*contracts.Message{{ID: 1}},
		boom,
	}
	this.waitErr = context.Canceled

	go this.subject.Listen()
	results := this.drain()

	this.So(this.recoverCalls, should.Equal, 2)
	this.So(len(results), should.Equal, 1) // the first page survives; it is already durable
	this.So(results[0].ID, should.Equal, uint64(1))
	assertBackoffWaits(this.Fixture, this.waits, 1)
	this.So(this.tracked, should.HaveLength, 2)
	failure, ok := this.tracked[0].(monitoring.RecoveryError)
	this.So(ok, should.BeTrue)
	this.So(failure.Attempt, should.Equal, 1)
	this.So(this.tracked[1], should.Equal, monitoring.RecoveryAbandoned{Attempts: 1})
}

func (this *RecoveryFixture) TestOversizedPageStillChunked() {
	this.script = []any{
		[]*contracts.Message{{ID: 1}, {ID: 2}}, // a page of 2 against batchSize 1
	}

	go this.subject.Listen()
	results := this.drain()

	this.So(this.units, should.Equal, []int{1, 1}) // split into batchSize-bounded units
	this.So(len(results), should.Equal, 2)
	this.So(this.tracked, should.Equal, []any{monitoring.RecoveryComplete{Count: 2}})
}
