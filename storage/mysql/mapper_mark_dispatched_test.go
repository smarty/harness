package mysql

import (
	"database/sql"

	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/internal/storage"
)

/* Tests and utilities for MarkMessagesDispatched operation */
func (this *MapperFixture) seedUndispatched() uint64 {
	result, err := this.handle.Exec(`INSERT INTO Messages (type, payload) VALUES ('order-received', '{}')`)
	this.So(err, should.BeNil)
	id, err := result.LastInsertId()
	this.So(err, should.BeNil)
	return uint64(id)
}
func (this *MapperFixture) seedDispatched() (id uint64, dispatched string) {
	result, err := this.handle.Exec(`INSERT INTO Messages (type, payload, dispatched) VALUES ('order-received', '{}', NOW(3))`)
	this.So(err, should.BeNil)
	rowID, err := result.LastInsertId()
	this.So(err, should.BeNil)
	return uint64(rowID), this.dispatchedTimestamp(uint64(rowID))
}
func (this *MapperFixture) dispatchedTimestamp(id uint64) string {
	var dispatched sql.NullString
	err := this.handle.QueryRow(`SELECT dispatched FROM Messages WHERE id = ?`, id).Scan(&dispatched)
	this.So(err, should.BeNil)
	return dispatched.String
}
func (this *MapperFixture) markMessagesDispatched(ids ...uint64) error {
	var messages []*contracts.Message
	for _, id := range ids {
		messages = append(messages, &contracts.Message{ID: id})
	}
	return this.subject.Exec(this.ctx, &storage.MarkMessagesDispatched{Messages: messages})
}
func (this *MapperFixture) TestMarkMessagesDispatched_SetsDispatchedTimestamp() {
	id := this.seedUndispatched()

	err := this.markMessagesDispatched(id)

	this.So(err, should.BeNil)
	this.So(this.dispatchedTimestamp(id), should.NOT.Equal, "")
}
func (this *MapperFixture) TestMarkMessagesDispatched_MultipleRows_AllMarked() {
	id1 := this.seedUndispatched()
	id2 := this.seedUndispatched()
	id3 := this.seedUndispatched()

	err := this.markMessagesDispatched(id1, id2, id3)

	this.So(err, should.BeNil)
	this.So(this.dispatchedTimestamp(id1), should.NOT.Equal, "")
	this.So(this.dispatchedTimestamp(id2), should.NOT.Equal, "")
	this.So(this.dispatchedTimestamp(id3), should.NOT.Equal, "")
}
func (this *MapperFixture) TestMarkMessagesDispatched_OnlyTouchesTargetedRows() {
	target := this.seedUndispatched()
	untouched := this.seedUndispatched()

	err := this.markMessagesDispatched(target)

	this.So(err, should.BeNil)
	this.So(this.dispatchedTimestamp(target), should.NOT.Equal, "")
	this.So(this.dispatchedTimestamp(untouched), should.Equal, "") // outside the id IN (...) set
}
func (this *MapperFixture) TestMarkMessagesDispatched_AlreadyDispatched_IsIdempotentAndPreservesTimestamp() {
	// The `dispatched IS NULL` guard makes re-marking idempotent: a row already
	// marked (e.g. by a prior delivery before a crash) keeps its original timestamp
	// and the operation does not error despite matching fewer rows than requested.

	id, original := this.seedDispatched()

	err := this.markMessagesDispatched(id)

	this.So(err, should.BeNil)
	this.So(this.dispatchedTimestamp(id), should.Equal, original)
}
func (this *MapperFixture) TestMarkMessagesDispatched_MixedNullAndNonNull_MarksOnlyTheNull() {
	fresh := this.seedUndispatched()
	already, original := this.seedDispatched()

	err := this.markMessagesDispatched(fresh, already)

	this.So(err, should.BeNil)
	this.So(this.dispatchedTimestamp(fresh), should.NOT.Equal, "")
	this.So(this.dispatchedTimestamp(already), should.Equal, original) // untouched by the IS NULL guard
}
func (this *MapperFixture) TestMarkMessagesDispatched_ReuseAcrossCalls_DoesNotLeakPriorBatch() {
	// Statements are drawn from a reused pool. Without a reset between calls a
	// recycled statement would carry the prior call's SQL fragment and arguments;
	// this exercises two distinct calls through the same Mapper to prove each is
	// built fresh.

	first := this.seedUndispatched()
	this.So(this.markMessagesDispatched(first), should.BeNil)

	second := this.seedUndispatched()
	third := this.seedUndispatched()
	this.So(this.markMessagesDispatched(second, third), should.BeNil)

	this.So(this.dispatchedTimestamp(first), should.NOT.Equal, "")
	this.So(this.dispatchedTimestamp(second), should.NOT.Equal, "")
	this.So(this.dispatchedTimestamp(third), should.NOT.Equal, "")
}
func (this *MapperFixture) TestMarkMessagesDispatched_ExecError_IsWrapped() {
	id := this.seedUndispatched()
	_ = this.handle.Close() // force the UPDATE to fail

	err := this.markMessagesDispatched(id)

	this.So(err, should.NOT.BeNil)
}
