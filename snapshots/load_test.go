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
	db     fakeStorage
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

// fakeStorage is a storage.Storage test double whose Exec calls are answered, in
// registration order, by the callbacks a test adds via prepareExec. Each callback
// type-asserts the operation it expects, checks inputs, fills in its Result, and
// returns the error the case requires.
type fakeStorage struct {
	callbacks []func(operation any) error
	calls     int
}

func (this *fakeStorage) Exec(_ context.Context, operation any) error {
	callback := this.callbacks[this.calls]
	this.calls++
	return callback(operation)
}

func (this *fakeStorage) prepareExec(callback func(operation any) error) {
	this.callbacks = append(this.callbacks, callback)
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

func (this *LoadFixture) TestNop() {
	this.So(func() { loadNop{}.Apply(nil) }, should.NOT.Panic)
	this.So(loadNop{}.Exec(nil, nil), should.BeNil)
}

func (this *LoadFixture) TestLatestPlainJSONLoadedAndAppliedToDomain() {
	this.db.prepareExec(func(a any) error {
		load := a.(*storage.LoadLatestSnapshot)
		load.Result = storage.LoadedSnapshotResult{
			Found:         true,
			HighWatermark: 42,
			Payload:       marshalJSON(domainState{Name: "alpha", Count: 7}),
		}
		return nil
	})
	spy := &applicatorSpy{}
	state := &domainState{}

	result, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(&this.db),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(state),
	)

	this.So(err, should.BeNil)
	this.So(result.PreviousHighWatermark, should.Equal, uint64(42))
	this.So(result.NewHighWatermark, should.Equal, uint64(42)) // no replay: stays at the snapshot watermark, not zero
	this.So(result.EventsAppliedCount, should.Equal, uint64(0))
	this.So(result.LoadedSnapshot, should.Equal, state)
	this.So(*state, should.Equal, domainState{Name: "alpha", Count: 7})
	// The domain is applied the de-referenced value, not the pointer:
	this.So(spy.applied, should.Equal, []any{domainState{Name: "alpha", Count: 7}})
	this.So(this.logged, should.Equal, []string{"[INFO] loaded snapshot at high watermark 42"})
}

func (this *LoadFixture) TestLatestGzipPayloadDecompressed() {
	raw := marshalJSON(domainState{Name: "beta", Count: 9})
	this.db.prepareExec(func(a any) error {
		a.(*storage.LoadLatestSnapshot).Result = storage.LoadedSnapshotResult{
			Found:           true,
			HighWatermark:   99,
			ContentEncoding: "gzip",
			Payload:         gzipPayload(raw),
		}
		return nil
	})
	spy := &applicatorSpy{}
	state := &domainState{}

	result, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(&this.db),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(state),
	)

	this.So(err, should.BeNil)
	this.So(result.PreviousHighWatermark, should.Equal, uint64(99))
	this.So(*state, should.Equal, domainState{Name: "beta", Count: 9})
}

func (this *LoadFixture) TestCorruptGzipReturnsError() {
	this.db.prepareExec(func(a any) error {
		a.(*storage.LoadLatestSnapshot).Result = storage.LoadedSnapshotResult{
			Found:           true,
			ContentEncoding: "gzip",
			Payload:         []byte("this is not gzip"),
		}
		return nil
	})
	spy := &applicatorSpy{}

	_, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(&this.db),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(&domainState{}),
	)

	this.So(err, should.NOT.BeNil)
	this.So(spy.applied, should.BeNil)
}

func (this *LoadFixture) TestInvalidJSONReturnsError() {
	this.db.prepareExec(func(a any) error {
		a.(*storage.LoadLatestSnapshot).Result = storage.LoadedSnapshotResult{
			Found:   true,
			Payload: []byte("{not valid json"),
		}
		return nil
	})
	spy := &applicatorSpy{}

	_, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(&this.db),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(&domainState{}),
	)

	this.So(err, should.NOT.BeNil)
	this.So(spy.applied, should.BeNil)
}

func (this *LoadFixture) TestSnapshotNotFoundReturnsMissingError() {
	this.db.prepareExec(func(a any) error {
		a.(*storage.LoadLatestSnapshot).Result = storage.LoadedSnapshotResult{Found: false}
		return nil
	})
	spy := &applicatorSpy{}

	result, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(&this.db),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(&domainState{}),
	)

	this.So(err, should.WrapError, errMissingSnapshot)
	this.So(result, should.Equal, LoadResult{})
	this.So(spy.applied, should.BeNil)
}

func (this *LoadFixture) TestSnapshotStorageErrorPropagates() {
	boom := fmt.Errorf("database unavailable")
	this.db.prepareExec(func(any) error { return boom })
	spy := &applicatorSpy{}

	_, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(&this.db),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(&domainState{}),
	)

	this.So(err, should.WrapError, boom)
	this.So(spy.applied, should.BeNil)
}

func (this *LoadFixture) TestSpecificSnapshotStorageErrorPropagates() {
	boom := fmt.Errorf("database unavailable")
	this.db.prepareExec(func(any) error { return boom })
	spy := &applicatorSpy{}

	_, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(&this.db),
		LoadOptions.SnapshotID(5),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(&domainState{}),
	)

	this.So(err, should.WrapError, boom)
	this.So(spy.applied, should.BeNil)
}

func (this *LoadFixture) TestSpecificSnapshotLoadedByID() {
	this.db.prepareExec(func(a any) error {
		load := a.(*storage.LoadSnapshot)
		this.So(load.ID, should.Equal, uint64(5))
		load.Result = storage.LoadedSnapshotResult{
			Found:         true,
			HighWatermark: 5,
			Payload:       marshalJSON(domainState{Name: "specific", Count: 3}),
		}
		return nil
	})
	spy := &applicatorSpy{}
	state := &domainState{}

	result, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(&this.db),
		LoadOptions.SnapshotID(5),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(state),
	)

	this.So(err, should.BeNil)
	this.So(result.PreviousHighWatermark, should.Equal, uint64(5))
	this.So(*state, should.Equal, domainState{Name: "specific", Count: 3})
}

func (this *LoadFixture) TestEventsSinceWatermarkAppliedAfterSnapshot() {
	this.db.prepareExec(func(a any) error {
		a.(*storage.LoadLatestSnapshot).Result = storage.LoadedSnapshotResult{
			Found:         true,
			HighWatermark: 3,
			Payload:       marshalJSON(domainState{Name: "snap", Count: 1}),
		}
		return nil
	})
	this.db.prepareExec(func(a any) error {
		query := a.(*storage.LoadEventsSince)
		// The event query reads from the loaded snapshot's high watermark:
		this.So(query.HighWatermark, should.Equal, uint64(3))
		query.Result.Events = []storage.Event{
			{Type: "event:alpha", Payload: marshalJSON(eventAlpha{Order: 11})},
			{Type: "event:beta", Payload: marshalJSON(eventBeta{Order: 22})},
		}
		query.Result.NewHighWatermark = 7
		return nil
	})
	spy := &applicatorSpy{}
	state := &domainState{}

	result, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(&this.db),
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
}

func (this *LoadFixture) TestNoRegistrySkipsEventQuery() {
	this.db.prepareExec(func(a any) error {
		a.(*storage.LoadLatestSnapshot).Result = storage.LoadedSnapshotResult{
			Found:         true,
			HighWatermark: 4,
			Payload:       marshalJSON(domainState{Name: "only-snapshot", Count: 0}),
		}
		return nil
	})
	spy := &applicatorSpy{}

	// No RegisteredEvents provided → replay is not enabled.
	result, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(&this.db),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(&domainState{}),
	)

	this.So(err, should.BeNil)
	this.So(this.db.calls, should.Equal, 1) // the events query was skipped
	this.So(result.EventsAppliedCount, should.Equal, uint64(0))
	this.So(result.NewHighWatermark, should.Equal, uint64(4)) // no replay: equals the snapshot watermark
	this.So(spy.applied, should.Equal, []any{domainState{Name: "only-snapshot", Count: 0}})
}

func (this *LoadFixture) TestRegistryButDomainAppliesNoEventsSkipsQuery() {
	this.db.prepareExec(func(a any) error {
		a.(*storage.LoadLatestSnapshot).Result = storage.LoadedSnapshotResult{
			Found:         true,
			HighWatermark: 4,
			Payload:       marshalJSON(domainState{Name: "snapshot-only", Count: 0}),
		}
		return nil
	})
	domain := &bareApplicator{}

	// Registry provided, but the Domain declares no typed Apply<Foo> methods.
	result, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(&this.db),
		LoadOptions.Domain(domain),
		LoadOptions.LoadedSnapshot(&domainState{}),
		LoadOptions.RegisteredEvents(registeredTypesByName(), registeredNamesByType()),
	)

	this.So(err, should.BeNil)
	this.So(this.db.calls, should.Equal, 1) // the events query was skipped
	this.So(result.EventsAppliedCount, should.Equal, uint64(0))
	this.So(result.NewHighWatermark, should.Equal, uint64(4)) // no replay: equals the snapshot watermark
	this.So(domain.applied, should.Equal, []any{domainState{Name: "snapshot-only", Count: 0}})
}

func (this *LoadFixture) TestDomainAppliesUnregisteredEventTypeReturnsError() {
	this.db.prepareExec(func(a any) error {
		a.(*storage.LoadLatestSnapshot).Result = storage.LoadedSnapshotResult{
			Found:   true,
			Payload: marshalJSON(domainState{Name: "snap", Count: 1}),
		}
		return nil
	})
	domain := &gammaApplicator{}

	// The Domain applies eventGamma, which is absent from the registry.
	result, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(&this.db),
		LoadOptions.Domain(domain),
		LoadOptions.LoadedSnapshot(&domainState{}),
		LoadOptions.RegisteredEvents(registeredTypesByName(), registeredNamesByType()),
	)

	this.So(err, should.WrapError, errUnregisteredEventType)
	this.So(this.db.calls, should.Equal, 1) // errored before querying storage
	this.So(result.EventsAppliedCount, should.Equal, uint64(0))
	// The snapshot is applied before events are resolved:
	this.So(domain.applied, should.Equal, []any{domainState{Name: "snap", Count: 1}})
}

func (this *LoadFixture) TestUnsupportedEventTypeReturnsError() {
	this.db.prepareExec(func(a any) error {
		a.(*storage.LoadLatestSnapshot).Result = storage.LoadedSnapshotResult{
			Found:   true,
			Payload: marshalJSON(domainState{Name: "snap", Count: 1}),
		}
		return nil
	})
	this.db.prepareExec(func(a any) error {
		a.(*storage.LoadEventsSince).Result.Events = []storage.Event{
			{Type: "event:unknown", Payload: marshalJSON(eventAlpha{Order: 1})},
		}
		return nil
	})
	spy := &applicatorSpy{}

	_, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(&this.db),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(&domainState{}),
		LoadOptions.RegisteredEvents(registeredTypesByName(), registeredNamesByType()),
	)

	this.So(err, should.WrapError, errUnsupportedMessageType)
	// The snapshot is applied before events are decoded:
	this.So(spy.applied, should.Equal, []any{domainState{Name: "snap", Count: 1}})
}

func (this *LoadFixture) TestCorruptEventPayloadReturnsError() {
	this.db.prepareExec(func(a any) error {
		a.(*storage.LoadLatestSnapshot).Result = storage.LoadedSnapshotResult{
			Found:   true,
			Payload: marshalJSON(domainState{Name: "snap", Count: 1}),
		}
		return nil
	})
	this.db.prepareExec(func(a any) error {
		a.(*storage.LoadEventsSince).Result.Events = []storage.Event{
			{Type: "event:alpha", Payload: []byte("{not valid json")},
		}
		return nil
	})
	spy := &applicatorSpy{}

	_, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(&this.db),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(&domainState{}),
		LoadOptions.RegisteredEvents(registeredTypesByName(), registeredNamesByType()),
	)

	this.So(err, should.NOT.BeNil)
}

func (this *LoadFixture) TestEventsStorageErrorPropagates() {
	boom := fmt.Errorf("events query failed")
	this.db.prepareExec(func(a any) error {
		a.(*storage.LoadLatestSnapshot).Result = storage.LoadedSnapshotResult{
			Found:   true,
			Payload: marshalJSON(domainState{Name: "snap", Count: 1}),
		}
		return nil
	})
	this.db.prepareExec(func(any) error { return boom })
	spy := &applicatorSpy{}

	_, err := Load(this.Context(),
		LoadOptions.Logger(this),
		LoadOptions.Storage(&this.db),
		LoadOptions.Domain(spy),
		LoadOptions.LoadedSnapshot(&domainState{}),
		LoadOptions.RegisteredEvents(registeredTypesByName(), registeredNamesByType()),
	)

	this.So(err, should.WrapError, boom)
}
