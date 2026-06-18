package mysql

import (
	"context"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/internal/storage"
)

func TestMapperInvalidTableFixture(t *testing.T) {
	gunit.Run(new(MapperInvalidTableFixture), t)
}

// MapperInvalidTableFixture exercises the error-handling branch each operation
// takes when quoteTableName rejects the configured table name. These paths
// bail out before touching the database handle, so a nil handle is never
// dereferenced and no MySQL server is required.
type MapperInvalidTableFixture struct {
	*gunit.Fixture
	ctx     context.Context
	subject *Mapper
}

func (this *MapperInvalidTableFixture) Setup() {
	this.ctx = this.Context()
	this.subject = NewMapper(nil, 1, "bad snapshots name", "bad messages name")
}

func (this *MapperInvalidTableFixture) TestMarkMessagesDispatched() {
	err := this.subject.Exec(this.ctx, &storage.MarkMessagesDispatched{})

	this.So(err, should.WrapError, errInvalidTableName)
}

func (this *MapperInvalidTableFixture) TestLoadUndispatchedBounds() {
	err := this.subject.Exec(this.ctx, &storage.LoadUndispatchedBounds{})

	this.So(err, should.WrapError, errInvalidTableName)
}

func (this *MapperInvalidTableFixture) TestLoadUndispatchedPage() {
	err := this.subject.Exec(this.ctx, &storage.LoadUndispatchedPage{})

	this.So(err, should.WrapError, errInvalidTableName)
}

func (this *MapperInvalidTableFixture) TestSaveSnapshot() {
	err := this.subject.Exec(this.ctx, &storage.SaveSnapshot{})

	this.So(err, should.WrapError, errInvalidTableName)
}

func (this *MapperInvalidTableFixture) TestLoadLatestSnapshot() {
	err := this.subject.Exec(this.ctx, &storage.LoadLatestSnapshot{})

	this.So(err, should.WrapError, errInvalidTableName)
}

func (this *MapperInvalidTableFixture) TestLoadSnapshot() {
	err := this.subject.Exec(this.ctx, &storage.LoadSnapshot{})

	this.So(err, should.WrapError, errInvalidTableName)
}

func (this *MapperInvalidTableFixture) TestLoadEventsSince() {
	err := this.subject.Exec(this.ctx, &storage.LoadEventsSince{})

	this.So(err, should.WrapError, errInvalidTableName)
}
