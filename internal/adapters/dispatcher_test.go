package adapters

import (
	"context"
	"errors"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/internal/storage"
)

type dispatcherTestEvent struct {
	AccountID uint64
	OrderID   uint64
}

func TestDispatcherFixture(t *testing.T) {
	gunit.Run(new(DispatcherFixture), t)
}

// DispatcherFixture exercises the Dispatcher in isolation: it stands in as the
// inner contracts.Dispatcher and supplies a fake storage.DB, so no real
// database is needed. The mark-dispatched SQL itself is covered by the Mapper's
// integration tests in internal/storage/mysql.
type DispatcherFixture struct {
	*gunit.Fixture
	ctx           context.Context
	db            *fakeDB
	subject       *Dispatcher
	dispatched    []*contracts.Message
	dispatchError error
}

// Dispatch records the published batch, standing in as the inner dispatcher.
func (this *DispatcherFixture) Dispatch(ctx context.Context, messages ...*contracts.Message) error {
	this.So(ctx.Value("testing"), should.Equal, this.Name())
	this.dispatched = append(this.dispatched, messages...)
	return this.dispatchError
}

func (this *DispatcherFixture) Setup() {
	this.ctx = context.WithValue(this.Context(), "testing", this.Name())
	this.dispatched = nil
	this.dispatchError = nil
	this.db = &fakeDB{expectedCtxValue: this.Name(), fixture: this.Fixture}
	this.subject = NewDispatcher(this, this.db)
}

func message(id uint64, value any) *contracts.Message {
	return &contracts.Message{
		ID:    id,
		Type:  "order-received",
		Value: value,
	}
}

func (this *DispatcherFixture) TestDispatch_PublishesThenMarksDispatched() {
	msg := message(42, dispatcherTestEvent{AccountID: 1, OrderID: 2})

	err := this.subject.Dispatch(this.ctx, msg)

	this.So(err, should.BeNil)
	this.So(this.dispatched, should.Equal, []*contracts.Message{msg})
	this.So(this.db.operations, should.Equal, []any{
		&storage.MarkMessagesDispatched{Messages: []*contracts.Message{msg}},
	})
}

func (this *DispatcherFixture) TestDispatch_MultipleMessages_MarkedInOneOperation() {
	first := message(42, dispatcherTestEvent{AccountID: 1, OrderID: 2})
	second := message(43, dispatcherTestEvent{AccountID: 3, OrderID: 4})

	err := this.subject.Dispatch(this.ctx, first, second)

	this.So(err, should.BeNil)
	this.So(this.dispatched, should.Equal, []*contracts.Message{first, second})
	this.So(this.db.operations, should.Equal, []any{
		&storage.MarkMessagesDispatched{Messages: []*contracts.Message{first, second}},
	})
}

func (this *DispatcherFixture) TestDispatch_PublishFails_ReturnsErrorWithoutMarkingDispatched() {
	this.dispatchError = errors.New("rmq down")
	msg := message(42, dispatcherTestEvent{AccountID: 1, OrderID: 2})

	err := this.subject.Dispatch(this.ctx, msg)

	this.So(err, should.WrapError, this.dispatchError)
	this.So(this.db.operations, should.BeEmpty)
}

func (this *DispatcherFixture) TestDispatch_MarkFails_ReturnsError() {
	this.db.err = errors.New("mark dispatched: connection refused")
	msg := message(42, dispatcherTestEvent{AccountID: 1, OrderID: 2})

	err := this.subject.Dispatch(this.ctx, msg)

	this.So(err, should.WrapError, this.db.err)
	this.So(this.dispatched, should.Equal, []*contracts.Message{msg}) // published before the mark failed
}

func (this *DispatcherFixture) TestDispatch_UnassignedID_RejectedBeforePublishing() {
	msg := message(0, dispatcherTestEvent{AccountID: 1, OrderID: 2})

	err := this.subject.Dispatch(this.ctx, msg)

	this.So(err, should.WrapError, errUnassignedID)
	this.So(this.dispatched, should.BeEmpty)
	this.So(this.db.operations, should.BeEmpty)
}

func (this *DispatcherFixture) TestDispatch_UnassignedIDAmongValidIDs_RejectsWholeBatch() {
	valid := message(42, dispatcherTestEvent{AccountID: 1, OrderID: 2})
	unassigned := message(0, dispatcherTestEvent{AccountID: 3, OrderID: 4})

	err := this.subject.Dispatch(this.ctx, valid, unassigned)

	this.So(err, should.WrapError, errUnassignedID)
	this.So(this.dispatched, should.BeEmpty)
	this.So(this.db.operations, should.BeEmpty)
}

func (this *DispatcherFixture) TestDispatch_NoMessages_NoOp() {
	err := this.subject.Dispatch(this.ctx)

	this.So(err, should.BeNil)
	this.So(this.dispatched, should.BeEmpty)
	this.So(this.db.operations, should.BeEmpty)
}

// fakeDB records every operation handed to Handle and can be primed to fail.
type fakeDB struct {
	fixture          *gunit.Fixture
	expectedCtxValue any
	operations       []any
	err              error
}

func (this *fakeDB) Handle(ctx context.Context, operation any) error {
	this.fixture.So(ctx.Value("testing"), should.Equal, this.expectedCtxValue)
	this.operations = append(this.operations, operation)
	return this.err
}
