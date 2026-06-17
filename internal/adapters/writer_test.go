package adapters

import (
	"context"
	"errors"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/internal/storage"
)

func TestWriterFixture(t *testing.T) {
	gunit.Run(new(WriterFixture), t)
}

// WriterFixture exercises the Writer in isolation with a fake contracts.Storage; no
// real database is needed. The INSERT, ID assignment, transaction, and the
// deprecated legacy hook are all covered by the Mapper's integration tests in
// internal/storage/mysql.
type WriterFixture struct {
	*gunit.Fixture
	ctx     context.Context
	db      *fakeDB
	subject *Writer
}

func (this *WriterFixture) Setup() {
	this.ctx = context.WithValue(this.Context(), "testing", this.Name())
	this.db = &fakeDB{expectedCtxValue: this.Name(), fixture: this.Fixture}
	this.subject = NewWriter(this.db)
}

func (this *WriterFixture) TestWrite_HandsInsertMessagesOperationToDB() {
	first := &contracts.Message{Type: "order-received"}
	second := &contracts.Message{Type: "order-approved"}

	err := this.subject.Write(this.ctx, first, second)

	this.So(err, should.BeNil)
	this.So(this.db.operations, should.Equal, []any{
		&storage.InsertMessages{Messages: []*contracts.Message{first, second}},
	})
}

func (this *WriterFixture) TestWrite_NoMessages_NoOp() {
	err := this.subject.Write(this.ctx)

	this.So(err, should.BeNil)
	this.So(this.db.operations, should.BeEmpty)
}

func (this *WriterFixture) TestWrite_DBError_Propagates() {
	this.db.err = errors.New("insert failed")

	err := this.subject.Write(this.ctx, &contracts.Message{Type: "order-received"})

	this.So(err, should.WrapError, this.db.err)
}
