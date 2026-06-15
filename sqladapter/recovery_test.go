package sqladapter

import (
	"context"
	"database/sql"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
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

// drain pages the subject with the given limit until an empty page, returning
// every recovered message flattened in dispatch order.
func (this *RecoveryFixture) drain(limit int) (results []*contracts.Message) {
	for {
		page, err := this.subject.Recover(this.ctx, limit)
		this.So(err, should.BeNil)
		if len(page) == 0 {
			return results
		}
		results = append(results, page...)
	}
}

func ids(messages []*contracts.Message) (results []uint64) {
	for _, message := range messages {
		results = append(results, message.ID)
	}
	return results
}

func (this *RecoveryFixture) TestRecover_NoOrphans_ReturnsNothing() {
	messages := this.drain(64)

	this.So(messages, should.BeEmpty)
}

func (this *RecoveryFixture) TestRecover_ReturnsUndispatchedRowsInIDOrder() {
	id1 := this.seedUndispatched("order-received", `{"order":1}`)
	id2 := this.seedUndispatched("order-approved", `{"order":2}`)
	_ = this.seedDispatched("order-received", `{"order":3}`) // already dispatched, must be skipped

	messages := this.drain(64)

	this.So(ids(messages), should.Equal, []uint64{id1, id2})
}

func (this *RecoveryFixture) TestRecover_PassesPayloadAndTypeIntoMessage() {
	id := this.seedUndispatched("order-received", `{"order":1}`)

	messages := this.drain(64)

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

	messages, err := this.subject.Recover(this.ctx, 64)

	this.So(err, should.NOT.BeNil)
	this.So(messages, should.BeEmpty)
}

func (this *RecoveryFixture) TestPaging_FivePagesOfTwo() {
	var seeded []uint64
	for range 5 {
		seeded = append(seeded, this.seedUndispatched("order-received", `{}`))
	}

	var lengths []int
	var got []uint64
	for {
		page, err := this.subject.Recover(this.ctx, 2)
		this.So(err, should.BeNil)
		lengths = append(lengths, len(page))
		got = append(got, ids(page)...)
		if len(page) == 0 {
			break
		}
	}

	this.So(lengths, should.Equal, []int{2, 2, 1, 0})
	this.So(got, should.Equal, seeded) // every row exactly once, strictly ascending
}

func (this *RecoveryFixture) TestDispatchedRowsSkippedAcrossPages() {
	id1 := this.seedUndispatched("order-received", `{}`)
	_ = this.seedDispatched("order-received", `{}`)
	id3 := this.seedUndispatched("order-received", `{}`)
	_ = this.seedDispatched("order-received", `{}`)
	id5 := this.seedUndispatched("order-received", `{}`)

	messages := this.drain(2)

	this.So(ids(messages), should.Equal, []uint64{id1, id3, id5})
}

func (this *RecoveryFixture) TestBoundarySnapshot_RowsInsertedAfterFirstCallNeverReturned() {
	early1 := this.seedUndispatched("order-received", `{}`)
	early2 := this.seedUndispatched("order-received", `{}`)

	// The first call snapshots the backlog boundary at early2.
	first, err := this.subject.Recover(this.ctx, 1)
	this.So(err, should.BeNil)
	this.So(ids(first), should.Equal, []uint64{early1})

	// Live traffic persists more undispatched rows during the recovery window.
	late := this.seedUndispatched("order-received", `{}`)

	messages := this.drain(1)

	this.So(ids(messages), should.Equal, []uint64{early2}) // late row excluded by the boundary
	this.So(ids(messages), should.NOT.Contain, late)
}

func (this *RecoveryFixture) TestEmptyBacklog_CompletesOnFirstCall() {
	_ = this.seedDispatched("order-received", `{}`)
	_ = this.seedDispatched("order-received", `{}`)

	page, err := this.subject.Recover(this.ctx, 64)

	this.So(err, should.BeNil)
	this.So(page, should.BeEmpty)
}

func (this *RecoveryFixture) TestFailedPageIsReServed() {
	id1 := this.seedUndispatched("order-received", `{}`)
	id2 := this.seedUndispatched("order-received", `{}`)

	cancelled, cancel := context.WithCancel(this.ctx)
	cancel()
	failed, err := this.subject.Recover(cancelled, 2)
	this.So(err, should.NOT.BeNil)
	this.So(failed, should.BeEmpty)

	// The cursor did not advance, so a good context re-serves the same page.
	page, err := this.subject.Recover(this.ctx, 2)
	this.So(err, should.BeNil)
	this.So(ids(page), should.Equal, []uint64{id1, id2})
}
