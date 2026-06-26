//go:build integration

package mysql

import (
	"github.com/smarty/gunit/v2/assert/should"
)

/* Tests for lazy, retry-friendly discovery of the server's auto_increment_increment. */

func (this *MapperFixture) TestStride_DetectedLazilyOnFirstWrite() {
	this.So(this.subject.stride.Load(), should.Equal, uint64(0)) // not yet detected before any write

	first := insertMessage(insertEvent{Order: 1}, "order-received")
	second := insertMessage(insertEvent{Order: 2}, "order-approved")
	err := this.insert(first, second)

	this.So(err, should.BeNil)
	this.So(this.subject.stride.Load(), should.Equal, this.autoIncrementIncrement())
	this.So(second.ID, should.Equal, first.ID+this.autoIncrementIncrement()) // IDs derived from the detected stride
}

func (this *MapperFixture) TestStride_DetectionFailure_ReturnedNotPanicked() {
	_ = this.handle.Close() // the detection query (before BeginTx) can no longer run

	err := this.insert(insertMessage(insertEvent{Order: 1}, "order-received"))

	this.So(err, should.NOT.BeNil)                               // surfaced for the persistence stage to retry
	this.So(this.subject.stride.Load(), should.Equal, uint64(0)) // nothing cached on failure
}
