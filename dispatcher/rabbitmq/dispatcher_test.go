package rabbitmq

import (
	"bytes"
	"context"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
)

func TestDispatcherFixture(t *testing.T) {
	gunit.Run(new(DispatcherFixture), t)
}

type DispatcherFixture struct {
	*gunit.Fixture
	ctx       context.Context
	transport *fakeTransport
	subject   *Dispatcher
}

func (this *DispatcherFixture) Setup() {
	this.ctx = context.Background()
	this.transport = &fakeTransport{}
	this.subject = &Dispatcher{transport: this.transport}
}

func message(typeName, body string) *contracts.Message {
	return &contracts.Message{
		ID:          1,
		Type:        typeName,
		ContentType: "application/json",
		Content:     bytes.NewBufferString(body),
	}
}

func (this *DispatcherFixture) TestWritesThenCommits() {
	err := this.subject.Dispatch(this.ctx,
		message("order-received", "a"),
		message("order-approved", "b"),
	)

	this.So(err, should.BeNil)
	this.So(this.transport.connects, should.Equal, 1)
	channel := this.transport.opened[0]
	this.So(channel.calls, should.Equal, []string{"publish", "publish", "commit"})
	this.So(channel.published[0].exchange, should.Equal, "order-received")
	this.So(channel.published[0].msg.Body, should.Equal, []byte("a"))
	this.So(channel.published[0].msg.DeliveryMode, should.Equal, uint8(amqp.Persistent))
	this.So(channel.published[1].exchange, should.Equal, "order-approved")
	this.So(channel.published[1].msg.Body, should.Equal, []byte("b"))
}

func (this *DispatcherFixture) TestEmptyBatch_NoConnectNoCommit() {
	err := this.subject.Dispatch(this.ctx)

	this.So(err, should.BeNil)
	this.So(this.transport.connects, should.Equal, 0)
	this.So(len(this.transport.opened), should.Equal, 0)
}

func (this *DispatcherFixture) TestReusesChannelAcrossDispatches() {
	this.So(this.subject.Dispatch(this.ctx, message("a", "1")), should.BeNil)
	this.So(this.subject.Dispatch(this.ctx, message("b", "2")), should.BeNil)

	this.So(this.transport.connects, should.Equal, 1)
	this.So(this.transport.opened[0].calls, should.Equal, []string{"publish", "commit", "publish", "commit"})
}

func (this *DispatcherFixture) TestPublishError_ResetsAndReconnectsNextDispatch() {
	boom := errors.New("publish boom")
	this.transport.channels = []*fakeChannel{{publishErr: boom}}

	err := this.subject.Dispatch(this.ctx, message("a", "1"))

	this.So(err, should.WrapError, boom)
	this.So(this.transport.opened[0].closes, should.Equal, 1)

	this.So(this.subject.Dispatch(this.ctx, message("a", "2")), should.BeNil)
	this.So(this.transport.connects, should.Equal, 2)
	this.So(this.transport.opened[1].calls, should.Equal, []string{"publish", "commit"})
}

func (this *DispatcherFixture) TestCommitError_ResetsAndReconnectsNextDispatch() {
	boom := errors.New("commit boom")
	this.transport.channels = []*fakeChannel{{commitErr: boom}}

	err := this.subject.Dispatch(this.ctx, message("a", "1"))

	this.So(err, should.WrapError, boom)
	this.So(this.transport.opened[0].closes, should.Equal, 1)

	this.So(this.subject.Dispatch(this.ctx, message("a", "2")), should.BeNil)
	this.So(this.transport.connects, should.Equal, 2)
}

func (this *DispatcherFixture) TestConnectError_ReturnedAndNothingCached() {
	boom := errors.New("connect boom")
	this.transport.connectErr = boom

	err := this.subject.Dispatch(this.ctx, message("a", "1"))

	this.So(err, should.WrapError, boom)
	this.So(this.transport.connects, should.Equal, 1)

	this.transport.connectErr = nil
	this.So(this.subject.Dispatch(this.ctx, message("a", "2")), should.BeNil)
	this.So(this.transport.connects, should.Equal, 2)
}

func (this *DispatcherFixture) TestClose_ReleasesCachedChannel() {
	this.So(this.subject.Dispatch(this.ctx, message("a", "1")), should.BeNil)

	this.So(this.subject.Close(), should.BeNil)
	this.So(this.transport.opened[0].closes, should.Equal, 1)
}

func (this *DispatcherFixture) TestClose_WithoutPriorDispatch_IsSafeNoOp() {
	this.So(this.subject.Close(), should.BeNil)
	this.So(this.transport.connects, should.Equal, 0)
}

// fakeTransport stands in for amqpTransport so the unit suite needs no broker.
// It hands out one channel per connect() call: the channels enqueued in
// `channels` are returned first (to inject failing channels), then healthy ones.
// Every channel handed out is retained in `opened` so a test can assert it was
// later closed by reset()/Close().
type fakeTransport struct {
	channels   []*fakeChannel
	opened     []*fakeChannel
	connectErr error
	connects   int
}

func (this *fakeTransport) connect(ctx context.Context) (channel, error) {
	this.connects++
	if this.connectErr != nil {
		return nil, this.connectErr
	}
	var next *fakeChannel
	if len(this.channels) > 0 {
		next, this.channels = this.channels[0], this.channels[1:]
	} else {
		next = &fakeChannel{}
	}
	this.opened = append(this.opened, next)
	return next, nil
}

// fakeChannel records the order of publish/commit calls, captures every
// published message, exposes injectable publish/commit errors, and counts closes.
type fakeChannel struct {
	calls      []string
	published  []publishedMessage
	publishErr error
	commitErr  error
	closes     int
}

type publishedMessage struct {
	exchange string
	msg      amqp.Publishing
}

func (this *fakeChannel) publish(ctx context.Context, exchange string, msg amqp.Publishing) error {
	this.calls = append(this.calls, "publish")
	this.published = append(this.published, publishedMessage{exchange: exchange, msg: msg})
	return this.publishErr
}
func (this *fakeChannel) commit() error {
	this.calls = append(this.calls, "commit")
	return this.commitErr
}
func (this *fakeChannel) close() error {
	this.closes++
	return nil
}
