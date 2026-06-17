package snapshots

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/internal/storage"
)

func TestSnapshotsFixture(t *testing.T) {
	gunit.Run(new(SnapshotsFixture), t)
}

type SnapshotsFixture struct {
	*gunit.Fixture
	logged []string
}

func (this *SnapshotsFixture) Printf(format string, args ...any) {
	this.logged = append(this.logged, fmt.Sprintf(format, args...))
}

type sampleSnapshot struct {
	Name  string
	Count int
}

func (this *SnapshotsFixture) TestLoadSnapshotPlainJSON() {
	original := sampleSnapshot{Name: "alpha", Count: 7}
	payload, err := json.Marshal(original)
	this.So(err, should.BeNil)

	loaded, err := LoadSnapshot[sampleSnapshot](this, payload, "", 42)

	this.So(err, should.BeNil)
	this.So(loaded, should.Equal, original)
}

func (this *SnapshotsFixture) TestLoadSnapshotGzipRoundTrip() {
	original := sampleSnapshot{Name: "beta", Count: 9}
	raw, err := json.Marshal(original)
	this.So(err, should.BeNil)

	loaded, err := LoadSnapshot[sampleSnapshot](this, gzipBytes(raw), "gzip", 99)

	this.So(err, should.BeNil)
	this.So(loaded, should.Equal, original)
}

func (this *SnapshotsFixture) TestLoadSnapshotCorruptGzipReturnsError() {
	loaded, err := LoadSnapshot[sampleSnapshot](this, []byte("this is not gzip"), "gzip", 1)

	this.So(err, should.NOT.BeNil)
	this.So(loaded, should.Equal, sampleSnapshot{})
}

func gzipBytes(data []byte) []byte {
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	_, _ = writer.Write(data)
	_ = writer.Close()
	return buffer.Bytes()
}

/* InitializeDomain fixtures */

type orderPlaced struct{ Order int }
type orderShipped struct{ Order int }

func messageTypes() map[string]reflect.Type {
	return map[string]reflect.Type{
		"order:placed":  reflect.TypeOf(orderPlaced{}),
		"order:shipped": reflect.TypeOf(orderShipped{}),
	}
}
func typeNames() map[reflect.Type]string {
	return map[reflect.Type]string{
		reflect.TypeOf(orderPlaced{}):  "order:placed",
		reflect.TypeOf(orderShipped{}): "order:shipped",
	}
}
func jsonOf(value any) []byte {
	raw, _ := json.Marshal(value)
	return raw
}

// fakeDB answers the snapshot/event operations from in-memory fixtures.
type fakeDB struct {
	snapshot         storage.LoadedSnapshotResult
	events           []storage.Event
	newHighWatermark uint64
	err              error
}

func (this *fakeDB) Exec(_ context.Context, operation any) error {
	if this.err != nil {
		return this.err
	}
	switch op := operation.(type) {
	case *storage.LoadLatestSnapshot:
		op.Result = this.snapshot
		return nil
	case *storage.LoadEventsSince:
		op.Result.Events = this.events
		op.Result.NewHighWatermark = this.newHighWatermark
		return nil
	default:
		return storage.ErrUnsupportedOperation
	}
}

type spyApplicator struct{ applied []any }

func (this *spyApplicator) Apply(message any) {
	this.applied = append(this.applied, message)
}

func (this *SnapshotsFixture) TestInitializeDomainAppliesSnapshotThenEvents() {
	db := &fakeDB{
		snapshot: storage.LoadedSnapshotResult{
			Found:         true,
			HighWatermark: 3,
			Payload:       jsonOf(sampleSnapshot{Name: "snap", Count: 1}),
		},
		events: []storage.Event{
			{Type: "order:placed", Payload: jsonOf(orderPlaced{Order: 11})},
			{Type: "order:shipped", Payload: jsonOf(orderShipped{Order: 22})},
		},
		newHighWatermark: 7,
	}
	spy := &spyApplicator{}

	report, err := InitializeDomain[sampleSnapshot](
		this.Context(), this, db, "Snapshots", messageTypes(), typeNames(), spy,
		orderPlaced{}, orderShipped{},
	)

	this.So(err, should.BeNil)
	this.So(report.PreviousHighWatermark, should.Equal, uint64(3))
	this.So(report.NewHighWatermark, should.Equal, uint64(7))
	this.So(report.EventsAppliedCount, should.Equal, uint64(2))
	this.So(spy.applied, should.Equal, []any{
		sampleSnapshot{Name: "snap", Count: 1},
		orderPlaced{Order: 11},
		orderShipped{Order: 22},
	})
}

func (this *SnapshotsFixture) TestInitializeDomainMissingSnapshotReports() {
	db := &fakeDB{snapshot: storage.LoadedSnapshotResult{Found: false}}
	spy := &spyApplicator{}

	report, err := InitializeDomain[sampleSnapshot](
		this.Context(), this, db, "Snapshots", messageTypes(), typeNames(), spy,
		orderPlaced{},
	)

	this.So(err, should.WrapError, errMissingSnapshot)
	this.So(report.EventsAppliedCount, should.Equal, uint64(0))
	this.So(spy.applied, should.BeNil)
}

func (this *SnapshotsFixture) TestInitializeDomainHandleErrorReported() {
	boom := fmt.Errorf("database unavailable")
	db := &fakeDB{err: boom}
	spy := &spyApplicator{}

	_, err := InitializeDomain[sampleSnapshot](
		this.Context(), this, db, "Snapshots", messageTypes(), typeNames(), spy,
		orderPlaced{},
	)

	this.So(err, should.WrapError, boom)
	this.So(spy.applied, should.BeNil)
}
