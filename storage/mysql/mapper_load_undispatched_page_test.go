//go:build integration

package mysql

import (
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/internal/storage"
)

/* Tests and utilities for LoadUndispatchedPage operation */

func (this *MapperFixture) loadPage(afterID, throughID uint64, limit int, types ...string) *storage.LoadUndispatchedPage {
	op := &storage.LoadUndispatchedPage{Types: types, AfterID: afterID, ThroughID: throughID, Limit: limit}
	this.So(this.subject.Exec(this.ctx, op), should.BeNil)
	return op
}

func (this *MapperFixture) TestLoadUndispatchedPage_ReturnsUndispatchedInIDOrder() {
	id1 := this.seedUndispatchedPayload("order-received", `{"order":1}`)
	id2 := this.seedUndispatchedPayload("order-approved", `{"order":2}`)

	op := this.loadPage(0, id2, 64)

	this.So(idsOf(op.Messages), should.Equal, []uint64{id1, id2})
}

func (this *MapperFixture) TestLoadUndispatchedPage_ExcludesDispatchedRows() {
	id1 := this.seedUndispatchedPayload("order-received", `{}`)
	_, _ = this.seedDispatched()
	id3 := this.seedUndispatchedPayload("order-received", `{}`)

	op := this.loadPage(0, id3, 64)

	this.So(idsOf(op.Messages), should.Equal, []uint64{id1, id3})
}

func (this *MapperFixture) TestLoadUndispatchedPage_RespectsWindowAndLimit() {
	id1 := this.seedUndispatchedPayload("order-received", `{}`)
	id2 := this.seedUndispatchedPayload("order-received", `{}`)
	id3 := this.seedUndispatchedPayload("order-received", `{}`)
	_ = this.seedUndispatchedPayload("order-received", `{}`) // beyond ThroughID

	// AfterID excludes id1; ThroughID excludes the fourth row; Limit caps at id2.
	op := this.loadPage(id1, id3, 1)

	this.So(idsOf(op.Messages), should.Equal, []uint64{id2})
}

func (this *MapperFixture) TestLoadUndispatchedPage_MapsRowFields() {
	id := this.seedUndispatchedPayload("order-received", `{"order":1}`)

	op := this.loadPage(0, id, 64)

	this.So(len(op.Messages), should.Equal, 1)
	message := op.Messages[0]
	this.So(message.ID, should.Equal, id)
	this.So(message.Type, should.Equal, "order-received")
	this.So(message.ContentType, should.Equal, "application/json")
	this.So(message.Content.Bytes(), should.Equal, []byte(`{"order":1}`))
	this.So(message.Value, should.BeNil)
}

func (this *MapperFixture) TestLoadUndispatchedPage_ScopedToTypes_ExcludesForeignTypes() {
	id1 := this.seedUndispatchedPayload("order-received", `{}`)
	_ = this.seedUndispatchedPayload("foreign-type", `{}`)
	id3 := this.seedUndispatchedPayload("order-approved", `{}`)
	last := this.seedUndispatchedPayload("foreign-type", `{}`)

	op := this.loadPage(0, last, 64, "order-approved", "order-received")

	this.So(idsOf(op.Messages), should.Equal, []uint64{id1, id3})
}

func (this *MapperFixture) TestLoadUndispatchedPage_ScopedToTypes_LimitCountsMatchingRowsOnly() {
	id1 := this.seedUndispatchedPayload("order-received", `{}`)
	_ = this.seedUndispatchedPayload("foreign-type", `{}`) // interleaved: must not consume the limit
	id3 := this.seedUndispatchedPayload("order-received", `{}`)
	last := this.seedUndispatchedPayload("order-received", `{}`)

	op := this.loadPage(0, last, 2, "order-received")

	this.So(idsOf(op.Messages), should.Equal, []uint64{id1, id3})
}

func idsOf(messages []*contracts.Message) (results []uint64) {
	for _, message := range messages {
		results = append(results, message.ID)
	}
	return results
}
