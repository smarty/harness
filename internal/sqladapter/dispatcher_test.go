package sqladapter

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
)

type dispatcherTestEvent struct {
	AccountID uint64
	OrderID   uint64
}

func TestDispatcherFixture(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running database tests.")
	}
	ensureDatabaseReadiness(t)
	gunit.Run(new(DispatcherFixture), t, gunit.Options.IntegrationTests())
}

type DispatcherFixture struct {
	*gunit.Fixture
	ctx           context.Context
	handle        *sql.DB
	subject       *Dispatcher
	dispatched    []*contracts.Message
	dispatchError error
}

func (this *DispatcherFixture) Dispatch(ctx context.Context, messages ...*contracts.Message) error {
	this.So(ctx.Value("testing"), should.Equal, this.Name())
	this.dispatched = append(this.dispatched, messages...)
	return this.dispatchError
}

func (this *DispatcherFixture) Setup() {
	this.ctx = context.WithValue(this.Context(), "testing", this.Name())
	this.dispatched = nil
	this.dispatchError = nil
	handle, err := openTestDatabase()
	this.So(err, should.BeNil)
	this.handle = handle
	_, err = handle.Exec(`TRUNCATE TABLE Messages;`)
	this.So(err, should.BeNil)
	this.subject = NewDispatcher(this, handle)
}

func (this *DispatcherFixture) Teardown() {
	_ = this.handle.Close()
}

func (this *DispatcherFixture) seedMessage(value any) *contracts.Message {
	result, err := this.handle.Exec(`INSERT INTO Messages (type, payload) VALUES ('order-received', '{}')`)
	this.So(err, should.BeNil)
	id, err := result.LastInsertId()
	this.So(err, should.BeNil)
	return &contracts.Message{
		ID:          uint64(id),
		Type:        "order-received",
		ContentType: "application/json",
		Content:     bytes.NewBufferString(`{}`),
		Value:       value,
	}
}

func (this *DispatcherFixture) TestDispatch_PublishesPreEncodedPayloadAndMetadata() {
	message := this.seedMessage(dispatcherTestEvent{AccountID: 1, OrderID: 2})

	err := this.subject.Dispatch(this.ctx, message)

	this.So(err, should.BeNil)
	this.So(this.dispatched, should.Equal, []*contracts.Message{message})
}

func (this *DispatcherFixture) TestDispatch_PublishesAndMarksDispatched() {
	event := dispatcherTestEvent{AccountID: 1, OrderID: 2}
	message := this.seedMessage(event)

	err := this.subject.Dispatch(this.ctx, message)

	this.So(err, should.BeNil)
	this.So(this.dispatched, should.Equal, []*contracts.Message{message})
	this.So(this.dispatchedTimestamp(message.ID), should.NOT.BeNil)
}

func (this *DispatcherFixture) TestDispatch_PublishFails_ReturnsErrorWithoutMarkingDispatched() {
	event := dispatcherTestEvent{AccountID: 1, OrderID: 2}
	message := this.seedMessage(event)
	this.dispatchError = errors.New("rmq down")

	err := this.subject.Dispatch(this.ctx, message)

	this.So(err, should.NOT.BeNil)
	this.So(this.dispatchedTimestamp(message.ID), should.BeNil)
}

func (this *DispatcherFixture) TestDispatch_NoMessages_NoOp() {
	err := this.subject.Dispatch(this.ctx)
	this.So(err, should.BeNil)
	this.So(this.dispatched, should.BeEmpty)
}

func (this *DispatcherFixture) dispatchedTimestamp(id uint64) *string {
	var dispatched sql.NullString
	err := this.handle.QueryRow(`SELECT dispatched FROM Messages WHERE id = ?`, id).Scan(&dispatched)
	this.So(err, should.BeNil)
	if dispatched.Valid {
		s := dispatched.String
		return &s
	}
	return nil
}
