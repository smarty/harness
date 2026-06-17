package mysql

import (
	"reflect"
	"time"

	"github.com/smarty/gunit/v2/assert/better"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/internal/storage"
)

/* Tests and utilities for the snapshot/event operations (SaveSnapshot, LoadLatestSnapshot, LoadEventsSince) */

func (this *MapperFixture) saveSnapshot(watermark uint64, payload string) {
	op := &storage.SaveSnapshot{
		TableName:       "Snapshots",
		Timestamp:       time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC),
		HighWatermark:   watermark,
		Payload:         []byte(payload),
		ContentType:     "application/json",
		ContentEncoding: "gzip",
	}
	this.So(this.subject.Handle(this.ctx, op), should.BeNil)
}

func (this *MapperFixture) loadLatestSnapshot() *storage.LoadLatestSnapshot {
	op := &storage.LoadLatestSnapshot{TableName: "Snapshots"}
	this.So(this.subject.Handle(this.ctx, op), should.BeNil)
	return op
}

func (this *MapperFixture) TestSaveThenLoadLatestSnapshot() {
	this.saveSnapshot(10, `{"v":1}`)
	this.saveSnapshot(20, `{"v":2}`)

	op := this.loadLatestSnapshot()

	this.So(op.Result.Found, should.BeTrue)
	this.So(op.Result.HighWatermark, should.Equal, uint64(20))
	this.So(op.Result.Payload, should.Equal, []byte(`{"v":2}`))
	this.So(op.Result.ContentType, should.Equal, "application/json")
	this.So(op.Result.ContentEncoding, should.Equal, "gzip")
}

func (this *MapperFixture) TestLoadLatestSnapshotEmptyTableReportsNotFound() {
	op := this.loadLatestSnapshot()

	this.So(op.Result.Found, should.BeFalse)
}

func (this *MapperFixture) TestSnapshotRejectsInvalidTableName() {
	save := &storage.SaveSnapshot{TableName: "Snap; DROP", Timestamp: time.Now(), Payload: []byte(`{}`)}
	this.So(this.subject.Handle(this.ctx, save), should.NOT.BeNil)

	load := &storage.LoadLatestSnapshot{TableName: "Snap; DROP"}
	this.So(this.subject.Handle(this.ctx, load), should.NOT.BeNil)

	// The rejected save must not have written a row.
	clean := this.loadLatestSnapshot()
	this.So(clean.Result.Found, should.BeFalse)
}

func (this *MapperFixture) seedMessage(messageType, payload string) uint64 {
	result, err := this.handle.Exec(`INSERT INTO Messages (type, payload) VALUES (?, ?)`, messageType, payload)
	this.So(err, should.BeNil)
	id, err := result.LastInsertId()
	this.So(err, should.BeNil)
	return uint64(id)
}

type sampleEventA struct{ Field string }
type sampleEventB struct{ Field string }

func (this *MapperFixture) TestLoadEventsSinceFiltersByTypeAndWatermark() {
	below := this.seedMessage("event:a", `{"x":0}`) // at/below the watermark, excluded
	_ = this.seedMessage("event:a", `{"x":1}`)
	_ = this.seedMessage("event:c", `{"x":2}`) // unwanted type, excluded
	idB := this.seedMessage("event:b", `{"x":3}`)

	op := &storage.LoadEventsSince{
		HighWatermark: below,
		Events:        []any{sampleEventA{}, sampleEventB{}},
		TypeNames: map[reflect.Type]string{
			reflect.TypeOf(sampleEventA{}): "event:a",
			reflect.TypeOf(sampleEventB{}): "event:b",
		},
	}
	this.So(this.subject.Handle(this.ctx, op), should.BeNil)

	this.So(len(op.Result.Events), better.Equal, 2)
	this.So(op.Result.Events[0], should.Equal, storage.Event{Type: "event:a", Payload: []byte(`{"x":1}`)})
	this.So(op.Result.Events[1], should.Equal, storage.Event{Type: "event:b", Payload: []byte(`{"x":3}`)})
	this.So(op.Result.NewHighWatermark, should.Equal, idB)
}

func (this *MapperFixture) TestLoadEventsSinceEmptyTypeSet() {
	_ = this.seedMessage("event:a", `{"x":1}`)

	op := &storage.LoadEventsSince{
		HighWatermark: 0,
		Events:        nil,
		TypeNames:     map[reflect.Type]string{},
	}
	// No types resolve: the handler errors out rather than emitting a malformed `IN ()`.
	this.So(this.subject.Handle(this.ctx, op), should.NOT.BeNil)

	this.So(len(op.Result.Events), should.Equal, 0)
	this.So(op.Result.NewHighWatermark, should.Equal, uint64(0))
}
