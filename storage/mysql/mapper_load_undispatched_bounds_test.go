package mysql

import (
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/internal/storage"
)

/* Tests and utilities for LoadUndispatchedBounds operations */

func (this *MapperFixture) loadBounds() *storage.LoadUndispatchedBounds {
	op := new(storage.LoadUndispatchedBounds)
	this.So(this.subject.Handle(this.ctx, op), should.BeNil)
	return op
}

func (this *MapperFixture) seedUndispatchedPayload(typeName, payload string) uint64 {
	result, err := this.handle.Exec(`INSERT INTO Messages (type, payload) VALUES (?, ?)`, typeName, payload)
	this.So(err, should.BeNil)
	id, err := result.LastInsertId()
	this.So(err, should.BeNil)
	return uint64(id)
}

func (this *MapperFixture) TestLoadUndispatchedBounds_EmptyTable_NotFound() {
	op := this.loadBounds()

	this.So(op.Found, should.BeFalse)
	this.So(op.Min, should.Equal, uint64(0))
	this.So(op.Max, should.Equal, uint64(0))
}

func (this *MapperFixture) TestLoadUndispatchedBounds_OnlyDispatched_NotFound() {
	_, _ = this.seedDispatched()
	_, _ = this.seedDispatched()

	op := this.loadBounds()

	this.So(op.Found, should.BeFalse)
}

func (this *MapperFixture) TestLoadUndispatchedBounds_SpansOnlyUndispatchedRows() {
	first := this.seedUndispatched()
	_, _ = this.seedDispatched() // higher id, but dispatched: must not raise Max
	last := this.seedUndispatched()

	op := this.loadBounds()

	this.So(op.Found, should.BeTrue)
	this.So(op.Min, should.Equal, first)
	this.So(op.Max, should.Equal, last)
}
