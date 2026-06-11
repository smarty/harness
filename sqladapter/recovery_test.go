package sqladapter

import (
	"context"
	"database/sql"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
)

func TestRecoveryFixture(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running database tests.")
	}
	ensureDatabaseReadiness(t)
	gunit.Run(new(RecoveryFixture), t, gunit.Options.IntegrationTests())
}

type RecoveryFixture struct {
	*gunit.Fixture
	ctx     context.Context
	handle  *sql.DB
	subject *Recovery
}

func (this *RecoveryFixture) Setup() {
	this.ctx = context.WithValue(this.Context(), "testing", this.Name())
	handle, err := openTestDatabase()
	this.So(err, should.BeNil)
	this.handle = handle
	_, err = handle.Exec(`TRUNCATE TABLE Messages;`)
	this.So(err, should.BeNil)
	this.subject = NewRecovery(handle)
}

func (this *RecoveryFixture) Teardown() {
	_ = this.handle.Close()
}

func (this *RecoveryFixture) seedUndispatched(typeName string, payload string) uint64 {
	result, err := this.handle.Exec(`INSERT INTO Messages (type, payload) VALUES (?, ?)`, typeName, payload)
	this.So(err, should.BeNil)
	id, err := result.LastInsertId()
	this.So(err, should.BeNil)
	return uint64(id)
}

func (this *RecoveryFixture) seedDispatched(typeName string, payload string) uint64 {
	result, err := this.handle.Exec(`INSERT INTO Messages (type, payload, dispatched) VALUES (?, ?, NOW(3))`, typeName, payload)
	this.So(err, should.BeNil)
	id, err := result.LastInsertId()
	this.So(err, should.BeNil)
	return uint64(id)
}

func (this *RecoveryFixture) TestRecover_NoOrphans_ReturnsNothing() {
	messages, err := this.subject.Recover(this.ctx)

	this.So(err, should.BeNil)
	this.So(messages, should.BeEmpty)
}

func (this *RecoveryFixture) TestRecover_ReturnsUndispatchedRowsInIDOrder() {
	id1 := this.seedUndispatched("order-received", `{"order":1}`)
	id2 := this.seedUndispatched("order-approved", `{"order":2}`)
	_ = this.seedDispatched("order-received", `{"order":3}`) // already dispatched, must be skipped

	messages, err := this.subject.Recover(this.ctx)

	this.So(err, should.BeNil)
	this.So(len(messages), should.Equal, 2)
	this.So(messages[0].ID, should.Equal, id1)
	this.So(messages[1].ID, should.Equal, id2)
}

func (this *RecoveryFixture) TestRecover_PassesPayloadAndTypeIntoMessage() {
	id := this.seedUndispatched("order-received", `{"order":1}`)

	messages, err := this.subject.Recover(this.ctx)

	this.So(err, should.BeNil)
	this.So(len(messages), should.Equal, 1)
	message := messages[0]
	this.So(message.ID, should.Equal, id)
	this.So(message.Type, should.Equal, "order-received")
	this.So(message.ContentType, should.Equal, "application/json")
	this.So(message.Content.Bytes(), should.Equal, []byte(`{"order":1}`))
	this.So(message.Value, should.BeNil)
}

func (this *RecoveryFixture) TestRecover_QueryError_ReturnsError() {
	this.seedUndispatched("order-received", `{"order":1}`)
	_ = this.handle.Close() // force query errors

	messages, err := this.subject.Recover(this.ctx)

	this.So(err, should.NOT.BeNil)
	this.So(messages, should.BeEmpty)
}
