package sqladapter

import (
	"context"
	"database/sql"
	"log"
	"os"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/internal/contracts"
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
	ctx           context.Context
	handle        *sql.DB
	dispatcher    *Dispatcher
	dispatcherErr error
	dispatched    []*contracts.Message
	writeBatches  []int
}

func (this *RecoveryFixture) Dispatch(ctx context.Context, messages ...*contracts.Message) error {
	this.So(ctx.Value("testing"), should.Equal, this.Name())
	this.dispatched = append(this.dispatched, messages...)
	this.writeBatches = append(this.writeBatches, len(messages))
	return this.dispatcherErr
}

func (this *RecoveryFixture) Setup() {
	this.ctx = context.WithValue(this.Context(), "testing", this.Name())
	this.dispatched = nil
	this.writeBatches = nil
	this.dispatcherErr = nil
	handle, err := openTestDatabase()
	this.So(err, should.BeNil)
	this.handle = handle
	_, err = handle.Exec(`TRUNCATE TABLE Messages;`)
	this.So(err, should.BeNil)
	this.dispatcher = NewDispatcher(this, handle)
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

func (this *RecoveryFixture) TestRecover_NoOrphans_NoOp() {
	err := Recover(this.ctx, this.handle, this.dispatcher, log.New(os.Stderr, "", 0), 1024)

	this.So(err, should.BeNil)
	this.So(len(this.dispatched), should.Equal, 0)
}

func (this *RecoveryFixture) TestRecover_DispatchesUndispatchedRowsInIDOrder() {
	id1 := this.seedUndispatched("order-received", `{"order":1}`)
	id2 := this.seedUndispatched("order-approved", `{"order":2}`)
	_ = this.seedDispatched("order-received", `{"order":3}`) // already dispatched, must be skipped

	err := Recover(this.ctx, this.handle, this.dispatcher, log.New(os.Stderr, "", 0), 1024)

	this.So(err, should.BeNil)
	this.So(len(this.dispatched), should.Equal, 2)
	this.So(this.dispatchedTimestamp(id1), should.NOT.BeNil)
	this.So(this.dispatchedTimestamp(id2), should.NOT.BeNil)
}

func (this *RecoveryFixture) TestRecover_PassesPayloadAndTypeIntoMessage() {
	this.seedUndispatched("order-received", `{"order":1}`)

	err := Recover(this.ctx, this.handle, this.dispatcher, log.New(os.Stderr, "", 0), 1024)

	this.So(err, should.BeNil)
	this.So(len(this.dispatched), should.Equal, 1)
	message := this.dispatched[0]
	this.So(message.ID, should.Equal, 1)
	this.So(message.Type, should.Equal, "order-received")
	this.So(message.ContentType, should.Equal, "application/json")
	this.So(message.Content.Bytes(), should.Equal, []byte(`{"order":1}`))
	this.So(message.Value, should.BeNil)
}

func (this *RecoveryFixture) TestRecover_PublishesDispatchWithTopicMessageTypeAndPayload() {
	this.seedUndispatched("order-received", `{"order":1}`)

	err := Recover(this.ctx, this.handle, this.dispatcher, log.New(os.Stderr, "", 0), 1024)

	this.So(err, should.BeNil)
	this.So(len(this.dispatched), should.Equal, 1)
	message := this.dispatched[0]
	this.So(message.Type, should.Equal, "order-received")
	this.So(message.ContentType, should.Equal, "application/json")
	this.So(message.Content.Bytes(), should.Equal, []byte(`{"order":1}`))
	this.So(message.Value, should.BeNil)
}

func (this *RecoveryFixture) TestRecover_RowsExceedBatchSize_FlushesInBatchesAndDispatchesAll() {
	const batchSize = 3
	const total = 7 // 3 + 3 + 1
	ids := make([]uint64, 0, total)
	for i := 0; i < total; i++ {
		ids = append(ids, this.seedUndispatched("order-received", `{}`))
	}

	err := Recover(this.ctx, this.handle, this.dispatcher, log.New(os.Stderr, "", 0), batchSize)

	this.So(err, should.BeNil)
	this.So(len(this.dispatched), should.Equal, total)
	for _, id := range ids {
		this.So(this.dispatchedTimestamp(id), should.NOT.BeNil)
	}
}

func (this *RecoveryFixture) TestRecover_RowCountIsMultipleOfBatchSize_FlushesUniformBatches() {
	const batchSize = 3
	const total = 6 // exactly 2 batches of 3
	for i := 0; i < total; i++ {
		this.seedUndispatched("order-received", `{}`)
	}

	err := Recover(this.ctx, this.handle, this.dispatcher, log.New(os.Stderr, "", 0), batchSize)

	this.So(err, should.BeNil)
	this.So(this.writeBatches, should.Equal, []int{3, 3})
}

func (this *RecoveryFixture) dispatchedTimestamp(id uint64) *string {
	var dispatched sql.NullString
	err := this.handle.QueryRow(`SELECT dispatched FROM Messages WHERE id = ?`, id).Scan(&dispatched)
	this.So(err, should.BeNil)
	if dispatched.Valid {
		s := dispatched.String
		return &s
	}
	return nil
}
