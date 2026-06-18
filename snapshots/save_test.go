package snapshots

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/internal/storage"
)

func TestSaveFixture(t *testing.T) {
	gunit.Run(new(SaveFixture), t)
}

type SaveFixture struct {
	*gunit.Fixture
}

// saveStorageSpy records the SaveSnapshot operation it was asked to execute.
type saveStorageSpy struct {
	captured *storage.SaveSnapshot
	called   bool
	err      error
}

func (this *saveStorageSpy) Exec(_ context.Context, operation any) error {
	this.called = true
	if this.err != nil {
		return this.err
	}
	switch op := operation.(type) {
	case *storage.SaveSnapshot:
		this.captured = op
		return nil
	default:
		return storage.ErrUnsupportedOperation
	}
}

func (this *SaveFixture) TestSnapshotMarshaledCompressedAndPersisted() {
	db := &saveStorageSpy{}
	timestamp := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)
	original := domainState{Name: "persist-me", Count: 13}

	err := Save(this.Context(),
		SaveOptions.Storage(db),
		SaveOptions.Timestamp(timestamp),
		SaveOptions.HighWatermark(77),
		SaveOptions.Snapshot(original),
	)

	this.So(err, should.BeNil)
	this.So(db.captured, should.NOT.BeNil)
	this.So(db.captured.Timestamp, should.Equal, timestamp)
	this.So(db.captured.HighWatermark, should.Equal, uint64(77))
	this.So(db.captured.ContentType, should.Equal, "application/json")
	this.So(db.captured.ContentEncoding, should.Equal, "gzip")

	// The payload must be gzip-compressed JSON that round-trips to the original:
	decompressed, err := gunzip(db.captured.Payload)
	this.So(err, should.BeNil)
	var roundTrip domainState
	this.So(json.Unmarshal(decompressed, &roundTrip), should.BeNil)
	this.So(roundTrip, should.Equal, original)
}

func (this *SaveFixture) TestTimestampDefaultsToUTCNow() {
	db := &saveStorageSpy{}

	err := Save(this.Context(),
		SaveOptions.Storage(db),
		SaveOptions.HighWatermark(1),
		SaveOptions.Snapshot(domainState{Name: "x"}),
	)

	this.So(err, should.BeNil)
	this.So(db.captured.Timestamp.IsZero(), should.BeFalse)
	this.So(db.captured.Timestamp.Location(), should.Equal, time.UTC)
}

func (this *SaveFixture) TestStorageErrorPropagates() {
	boom := fmt.Errorf("write failed")
	db := &saveStorageSpy{err: boom}

	err := Save(this.Context(),
		SaveOptions.Storage(db),
		SaveOptions.Snapshot(domainState{Name: "x"}),
	)

	this.So(err, should.WrapError, boom)
}

func (this *SaveFixture) TestUnmarshalableSnapshotReturnsErrorWithoutStoring() {
	db := &saveStorageSpy{}

	err := Save(this.Context(),
		SaveOptions.Storage(db),
		SaveOptions.Snapshot(make(chan int)), // channels cannot be JSON-encoded
	)

	this.So(err, should.NOT.BeNil)
	this.So(db.called, should.BeFalse)
}
