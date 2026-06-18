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

func TestLoadFixture(t *testing.T) {
	gunit.Run(new(LoadFixture), t)
}

type LoadFixture struct {
	*gunit.Fixture
	logged []string
}

func (this *LoadFixture) Printf(format string, args ...any) {
	this.logged = append(this.logged, fmt.Sprintf(format, args...))
}

/* domain fixtures shared by load_test.go and save_test.go */

type domainState struct {
	Name  string
	Count int
}

type eventAlpha struct{ Order int }
type eventBeta struct{ Order int }

func registeredTypesByName() map[string]reflect.Type {
	return map[string]reflect.Type{
		"event:alpha": reflect.TypeOf(eventAlpha{}),
		"event:beta":  reflect.TypeOf(eventBeta{}),
	}
}
func registeredNamesByType() map[reflect.Type]string {
	return map[reflect.Type]string{
		reflect.TypeOf(eventAlpha{}): "event:alpha",
		reflect.TypeOf(eventBeta{}):  "event:beta",
	}
}

func marshalJSON(value any) []byte {
	raw, _ := json.Marshal(value)
	return raw
}
func gzipPayload(data []byte) []byte {
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	_, _ = writer.Write(data)
	_ = writer.Close()
	return buffer.Bytes()
}

// loadStorageStub answers the snapshot/event operations from in-memory fixtures
// and records the operations it received for assertions.
type loadStorageStub struct {
	latest           storage.LoadedSnapshotResult
	byID             storage.LoadedSnapshotResult
	events           []storage.Event
	newHighWatermark uint64

	snapshotErr error
	eventsErr   error

	capturedID    uint64
	capturedQuery *storage.LoadEventsSince
}

func (this *loadStorageStub) Exec(_ context.Context, operation any) error {
	switch op := operation.(type) {
	case *storage.LoadLatestSnapshot:
		if this.snapshotErr != nil {
			return this.snapshotErr
		}
		op.Result = this.latest
		return nil
	case *storage.LoadSnapshot:
		if this.snapshotErr != nil {
			return this.snapshotErr
		}
		this.capturedID = op.ID
		op.Result = this.byID
		return nil
	case *storage.LoadEventsSince:
		if this.eventsErr != nil {
			return this.eventsErr
		}
		this.capturedQuery = op
		op.Result.Events = this.events
		op.Result.NewHighWatermark = this.newHighWatermark
		return nil
	default:
		return storage.ErrUnsupportedOperation
	}
}

// applicatorSpy records every applied message through the generic Apply, while
// its typed Apply<Foo> methods exist solely so domainscan can discover which
// event types this domain handles.
type applicatorSpy struct{ applied []any }

func (this *applicatorSpy) Apply(message any) {
	this.applied = append(this.applied, message)
}
func (this *applicatorSpy) ApplyEventAlpha(eventAlpha) {}
func (this *applicatorSpy) ApplyEventBeta(eventBeta)   {}

// bareApplicator handles the snapshot but declares no typed event handlers.
type bareApplicator struct{ applied []any }

func (this *bareApplicator) Apply(message any) {
	this.applied = append(this.applied, message)
}

// eventGamma is deliberately absent from the registered-events maps.
type eventGamma struct{ Order int }

// gammaApplicator applies an event type that is not registered.
type gammaApplicator struct{ applied []any }

func (this *gammaApplicator) Apply(message any) {
	this.applied = append(this.applied, message)
}
func (this *gammaApplicator) ApplyEventGamma(eventGamma) {}

func (this *LoadFixture) TestLatestPlainJSONLoadedAndAppliedToDomain() {
	db := &loadStorageStub{latest: storage.LoadedSnapshotResult{
		Found:         true,
		HighWatermark: 42,
		Payload:       marshalJSON(domainState{Name: "alpha", Count: 7}),
	}}
	spy := &applicatorSpy{}
	state := &domainState{}

	result, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(db),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(state),
	)

	this.So(err, should.BeNil)
	this.So(result.PreviousHighWatermark, should.Equal, uint64(42))
	this.So(result.NewHighWatermark, should.Equal, uint64(0))
	this.So(result.EventsAppliedCount, should.Equal, uint64(0))
	this.So(result.LoadedSnapshot, should.Equal, state)
	this.So(*state, should.Equal, domainState{Name: "alpha", Count: 7})
	// The domain is applied the de-referenced value, not the pointer:
	this.So(spy.applied, should.Equal, []any{domainState{Name: "alpha", Count: 7}})
	this.So(this.logged, should.Equal, []string{"[INFO] loaded snapshot at high watermark 42"})
}

func (this *LoadFixture) TestLatestGzipPayloadDecompressed() {
	raw := marshalJSON(domainState{Name: "beta", Count: 9})
	db := &loadStorageStub{latest: storage.LoadedSnapshotResult{
		Found:           true,
		HighWatermark:   99,
		ContentEncoding: "gzip",
		Payload:         gzipPayload(raw),
	}}
	spy := &applicatorSpy{}
	state := &domainState{}

	result, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(db),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(state),
	)

	this.So(err, should.BeNil)
	this.So(result.PreviousHighWatermark, should.Equal, uint64(99))
	this.So(*state, should.Equal, domainState{Name: "beta", Count: 9})
}

func (this *LoadFixture) TestCorruptGzipReturnsError() {
	db := &loadStorageStub{latest: storage.LoadedSnapshotResult{
		Found:           true,
		ContentEncoding: "gzip",
		Payload:         []byte("this is not gzip"),
	}}
	spy := &applicatorSpy{}

	_, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(db),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(&domainState{}),
	)

	this.So(err, should.NOT.BeNil)
	this.So(spy.applied, should.BeNil)
}

func (this *LoadFixture) TestInvalidJSONReturnsError() {
	db := &loadStorageStub{latest: storage.LoadedSnapshotResult{
		Found:   true,
		Payload: []byte("{not valid json"),
	}}
	spy := &applicatorSpy{}

	_, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(db),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(&domainState{}),
	)

	this.So(err, should.NOT.BeNil)
	this.So(spy.applied, should.BeNil)
}

func (this *LoadFixture) TestSnapshotNotFoundReturnsMissingError() {
	db := &loadStorageStub{latest: storage.LoadedSnapshotResult{Found: false}}
	spy := &applicatorSpy{}

	result, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(db),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(&domainState{}),
	)

	this.So(err, should.WrapError, errMissingSnapshot)
	this.So(result, should.Equal, LoadResult{})
	this.So(spy.applied, should.BeNil)
}

func (this *LoadFixture) TestSnapshotStorageErrorPropagates() {
	boom := fmt.Errorf("database unavailable")
	db := &loadStorageStub{snapshotErr: boom}
	spy := &applicatorSpy{}

	_, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(db),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(&domainState{}),
	)

	this.So(err, should.WrapError, boom)
	this.So(spy.applied, should.BeNil)
}

func (this *LoadFixture) TestSpecificSnapshotLoadedByID() {
	db := &loadStorageStub{byID: storage.LoadedSnapshotResult{
		Found:         true,
		HighWatermark: 5,
		Payload:       marshalJSON(domainState{Name: "specific", Count: 3}),
	}}
	spy := &applicatorSpy{}
	state := &domainState{}

	result, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(db),
		LoadOptions.SnapshotID(5),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(state),
	)

	this.So(err, should.BeNil)
	this.So(db.capturedID, should.Equal, uint64(5))
	this.So(result.PreviousHighWatermark, should.Equal, uint64(5))
	this.So(*state, should.Equal, domainState{Name: "specific", Count: 3})
}

func (this *LoadFixture) TestEventsSinceWatermarkAppliedAfterSnapshot() {
	db := &loadStorageStub{
		latest: storage.LoadedSnapshotResult{
			Found:         true,
			HighWatermark: 3,
			Payload:       marshalJSON(domainState{Name: "snap", Count: 1}),
		},
		events: []storage.Event{
			{Type: "event:alpha", Payload: marshalJSON(eventAlpha{Order: 11})},
			{Type: "event:beta", Payload: marshalJSON(eventBeta{Order: 22})},
		},
		newHighWatermark: 7,
	}
	spy := &applicatorSpy{}
	state := &domainState{}

	result, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(db),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(state),
		LoadOptions.RegisteredEvents(registeredTypesByName(), registeredNamesByType()),
	)

	this.So(err, should.BeNil)
	this.So(result.PreviousHighWatermark, should.Equal, uint64(3))
	this.So(result.NewHighWatermark, should.Equal, uint64(7))
	this.So(result.EventsAppliedCount, should.Equal, uint64(2))
	this.So(spy.applied, should.Equal, []any{
		domainState{Name: "snap", Count: 1},
		eventAlpha{Order: 11},
		eventBeta{Order: 22},
	})
	// The event query reads from the loaded snapshot's high watermark:
	this.So(db.capturedQuery.HighWatermark, should.Equal, uint64(3))
}

func (this *LoadFixture) TestNoRegistrySkipsEventQuery() {
	db := &loadStorageStub{latest: storage.LoadedSnapshotResult{
		Found:         true,
		HighWatermark: 4,
		Payload:       marshalJSON(domainState{Name: "only-snapshot", Count: 0}),
	}}
	spy := &applicatorSpy{}

	// No RegisteredEvents provided → replay is not enabled.
	result, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(db),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(&domainState{}),
	)

	this.So(err, should.BeNil)
	this.So(db.capturedQuery, should.BeNil)
	this.So(result.EventsAppliedCount, should.Equal, uint64(0))
	this.So(spy.applied, should.Equal, []any{domainState{Name: "only-snapshot", Count: 0}})
}

func (this *LoadFixture) TestRegistryButDomainAppliesNoEventsSkipsQuery() {
	db := &loadStorageStub{latest: storage.LoadedSnapshotResult{
		Found:         true,
		HighWatermark: 4,
		Payload:       marshalJSON(domainState{Name: "snapshot-only", Count: 0}),
	}}
	domain := &bareApplicator{}

	// Registry provided, but the Domain declares no typed Apply<Foo> methods.
	result, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(db),
		LoadOptions.Domain(domain),
		LoadOptions.LoadedSnapshot(&domainState{}),
		LoadOptions.RegisteredEvents(registeredTypesByName(), registeredNamesByType()),
	)

	this.So(err, should.BeNil)
	this.So(db.capturedQuery, should.BeNil)
	this.So(result.EventsAppliedCount, should.Equal, uint64(0))
	this.So(domain.applied, should.Equal, []any{domainState{Name: "snapshot-only", Count: 0}})
}

func (this *LoadFixture) TestDomainAppliesUnregisteredEventTypeReturnsError() {
	db := &loadStorageStub{latest: storage.LoadedSnapshotResult{
		Found:   true,
		Payload: marshalJSON(domainState{Name: "snap", Count: 1}),
	}}
	domain := &gammaApplicator{}

	// The Domain applies eventGamma, which is absent from the registry.
	result, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(db),
		LoadOptions.Domain(domain),
		LoadOptions.LoadedSnapshot(&domainState{}),
		LoadOptions.RegisteredEvents(registeredTypesByName(), registeredNamesByType()),
	)

	this.So(err, should.WrapError, errUnregisteredEventType)
	this.So(db.capturedQuery, should.BeNil) // errored before querying storage
	this.So(result.EventsAppliedCount, should.Equal, uint64(0))
	// The snapshot is applied before events are resolved:
	this.So(domain.applied, should.Equal, []any{domainState{Name: "snap", Count: 1}})
}

func (this *LoadFixture) TestUnsupportedEventTypeReturnsError() {
	db := &loadStorageStub{
		latest: storage.LoadedSnapshotResult{
			Found:   true,
			Payload: marshalJSON(domainState{Name: "snap", Count: 1}),
		},
		events: []storage.Event{
			{Type: "event:unknown", Payload: marshalJSON(eventAlpha{Order: 1})},
		},
	}
	spy := &applicatorSpy{}

	_, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(db),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(&domainState{}),
		LoadOptions.RegisteredEvents(registeredTypesByName(), registeredNamesByType()),
	)

	this.So(err, should.WrapError, errUnsupportedMessageType)
	// The snapshot is applied before events are decoded:
	this.So(spy.applied, should.Equal, []any{domainState{Name: "snap", Count: 1}})
}

func (this *LoadFixture) TestCorruptEventPayloadReturnsError() {
	db := &loadStorageStub{
		latest: storage.LoadedSnapshotResult{
			Found:   true,
			Payload: marshalJSON(domainState{Name: "snap", Count: 1}),
		},
		events: []storage.Event{
			{Type: "event:alpha", Payload: []byte("{not valid json")},
		},
	}
	spy := &applicatorSpy{}

	_, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(db),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(&domainState{}),
		LoadOptions.RegisteredEvents(registeredTypesByName(), registeredNamesByType()),
	)

	this.So(err, should.NOT.BeNil)
}

func (this *LoadFixture) TestEventsStorageErrorPropagates() {
	boom := fmt.Errorf("events query failed")
	db := &loadStorageStub{
		latest: storage.LoadedSnapshotResult{
			Found:   true,
			Payload: marshalJSON(domainState{Name: "snap", Count: 1}),
		},
		eventsErr: boom,
	}
	spy := &applicatorSpy{}

	_, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(db),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(&domainState{}),
		LoadOptions.RegisteredEvents(registeredTypesByName(), registeredNamesByType()),
	)

	this.So(err, should.WrapError, boom)
}
