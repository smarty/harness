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
	ctx     context.Context
	output  chan *unitOfWork
	waits   []time.Duration
	waitErr error
	tracked []any
	subject *Recovery

	recovered    []*contracts.Message
	recoverErrs  []error
	recoverCalls int
}

func (this *RecoveryFixture) Setup() {
	this.ctx = context.WithValue(this.Context(), "testing", this.Name())
	this.output = make(chan *unitOfWork, 4)
	this.subject = newRecovery(this.ctx, this, 1, this.output, this.wait, this)
}

func (this *RecoveryFixture) wait(_ context.Context, d time.Duration) error {
	this.waits = append(this.waits, d)
	return this.waitErr
}

func (this *RecoveryFixture) Track(observation any) {
	this.tracked = append(this.tracked, observation)
}

func (this *RecoveryFixture) Recover(ctx context.Context) ([]*contracts.Message, error) {
	this.So(ctx.Value("testing"), should.Equal, this.Name())
	this.recoverCalls++
	if len(this.recoverErrs) > 0 {
		err := this.recoverErrs[0]
		this.recoverErrs = this.recoverErrs[1:]
		return nil, err
	}
	return this.recovered, nil
}

func (this *RecoveryFixture) drain() (results []*contracts.Message) {
	for unit := range this.output {
		results = append(results, unit.results...)
	}
	return results
}

func (this *RecoveryFixture) TestNothingToRecover_EmitsNoUnitAndClosesOutput() {
	go this.subject.Listen()
	results := this.drain()

	this.So(results, should.BeEmpty)
	this.So(this.waits, should.BeEmpty)
	this.So(this.tracked, should.BeEmpty)
}

func (this *RecoveryFixture) TestRecoveredMessages_EmittedAsSingleUnitThenOutputClosed() {
	this.recovered = []*contracts.Message{{ID: 1}, {ID: 2}}

	go this.subject.Listen()
	results := this.drain()

	this.So(results, should.Equal, this.recovered)
	this.So(this.waits, should.BeEmpty)
	this.So(this.tracked, should.BeEmpty)
}

func (this *RecoveryFixture) TestRecoverError_TracksThenWaitsThenRetries() {
	boom := errors.New("boom")
	this.recoverErrs = []error{boom, boom}
	this.recovered = []*contracts.Message{{ID: 1}}

	go this.subject.Listen()
	results := this.drain()

	this.So(results, should.Equal, this.recovered)
	this.So(this.recoverCalls, should.Equal, 3)
	this.So(this.waits, should.Equal, []time.Duration{time.Second, time.Second})
	this.So(this.tracked, should.Equal, []any{
		monitoring.RecoveryError{Attempts: 1, Error: boom},
		monitoring.RecoveryError{Attempts: 2, Error: boom},
	})
}

func (this *RecoveryFixture) TestRecoverError_WaitFails_AbandonsAndClosesOutput() {
	boom := errors.New("boom")
	this.recoverErrs = []error{boom}
	this.recovered = []*contracts.Message{{ID: 1}} // would succeed on retry, but wait fails first
	this.waitErr = context.Canceled

	go this.subject.Listen()
	results := this.drain()

	this.So(results, should.BeEmpty)
	this.So(this.recoverCalls, should.Equal, 1)
	this.So(this.waits, should.Equal, []time.Duration{time.Second})
	this.So(this.tracked, should.Equal, []any{monitoring.RecoveryError{Attempts: 1, Error: boom}})
}
