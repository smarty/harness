package snapshots

import (
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
	db fakeStorage
}

func (this *SaveFixture) TestSnapshotMarshaledCompressedAndPersisted() {
	timestamp := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)
	original := domainState{Name: "persist-me", Count: 13}
	this.db.prepareExec(func(a any) error {
		save := a.(*storage.SaveSnapshot)
		this.So(save.Timestamp, should.Equal, timestamp)
		this.So(save.HighWatermark, should.Equal, uint64(77))
		this.So(save.ContentType, should.Equal, "application/json")
		this.So(save.ContentEncoding, should.Equal, "gzip")
		// The payload must be gzip-compressed JSON that round-trips to the original:
		decompressed, err := gunzip(save.Payload)
		this.So(err, should.BeNil)
		var roundTrip domainState
		this.So(json.Unmarshal(decompressed, &roundTrip), should.BeNil)
		this.So(roundTrip, should.Equal, original)
		return nil
	})

	err := Save(this.Context(),
		SaveOptions.Storage(&this.db),
		SaveOptions.Timestamp(timestamp),
		SaveOptions.HighWatermark(77),
		SaveOptions.Snapshot(original),
	)

	this.So(err, should.BeNil)
	this.So(this.db.calls, should.Equal, 1)
}

func (this *SaveFixture) TestTimestampDefaultsToUTCNow() {
	this.db.prepareExec(func(a any) error {
		save := a.(*storage.SaveSnapshot)
		this.So(save.Timestamp.IsZero(), should.BeFalse)
		this.So(save.Timestamp.Location(), should.Equal, time.UTC)
		return nil
	})

	err := Save(this.Context(),
		SaveOptions.Storage(&this.db),
		SaveOptions.HighWatermark(1),
		SaveOptions.Snapshot(domainState{Name: "x"}),
	)

	this.So(err, should.BeNil)
}

func (this *SaveFixture) TestStorageErrorPropagates() {
	boom := fmt.Errorf("write failed")
	this.db.prepareExec(func(any) error { return boom })

	err := Save(this.Context(),
		SaveOptions.Storage(&this.db),
		SaveOptions.Snapshot(domainState{Name: "x"}),
	)

	this.So(err, should.WrapError, boom)
}

func (this *SaveFixture) TestUnmarshalableSnapshotReturnsErrorWithoutStoring() {
	err := Save(this.Context(),
		SaveOptions.Storage(&this.db),
		SaveOptions.Snapshot(make(chan int)), // channels cannot be JSON-encoded
	)

	this.So(err, should.NOT.BeNil)
	this.So(this.db.calls, should.Equal, 0)
}
