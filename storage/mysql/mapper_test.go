package mysql

import (
	"context"
	"database/sql"
	"log"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/internal/storage"
)

func TestMapperFixture(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running database tests.")
	}
	ensureDatabaseReadiness(t)
	gunit.Run(new(MapperFixture), t, gunit.Options.IntegrationTests())
}

type MapperFixture struct {
	*gunit.Fixture
	ctx     context.Context
	handle  *sql.DB
	stride  uint64
	subject *Mapper
}

func (this *MapperFixture) Setup() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(this.Output())
	this.ctx = this.Context()
	handle, err := openTestDatabase()
	this.So(err, should.BeNil)
	this.handle = handle
	_, err = handle.Exec(`TRUNCATE TABLE Messages;`)
	this.So(err, should.BeNil)
	_, err = handle.Exec(`TRUNCATE TABLE Snapshots;`)
	this.So(err, should.BeNil)
	this.stride = this.autoIncrementIncrement()
	this.subject = NewMapper(handle, this.stride)
}

func (this *MapperFixture) autoIncrementIncrement() uint64 {
	var result uint64
	err := this.handle.QueryRow(`SELECT @@auto_increment_increment`).Scan(&result)
	this.So(err, should.BeNil)
	return result
}

func (this *MapperFixture) Teardown() {
	_ = this.handle.Close()
}

func (this *MapperFixture) TestHandle_UnsupportedOperation_ReturnsError() {
	err := this.subject.Exec(this.ctx, "not a known operation")

	this.So(err, should.WrapError, storage.ErrUnsupportedOperation)
}
