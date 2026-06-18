package snapshots

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"reflect"

	"github.com/smarty/harness/v2/internal/domainscan"
	"github.com/smarty/harness/v2/internal/storage"
)

// Load is the mechanism to:
// 1. retrieve a snapshot (specified or latest) from storage, then
// 2. apply that snapshot to a provided instance of the domain, then
// 3. apply events since the high watermark of the loaded snapshot (if desired).
//
// A result struct is provided with the loaded snapshot, as well as info
// about the previous and new high watermarks and how many events were applied
// to the domain.
//
// From there callers may decide to:
//  1. Export a subsequent (updated) snapshot for custom queries/reports,
//  2. Export a subsequent (updated) snapshot to be persisted to storage,
//     most likely via this Save, and/or
//  3. Provide the applied domain to make business decisions, perhaps using
//     the pipeline provided by github.com/smarty/harness/v2
func Load(ctx context.Context, options ...loadOption) (LoadResult, error) {
	var config loadConfig
	for _, option := range append(LoadOptions.defaults(), options...) {
		option(&config)
	}
	return load(ctx, config)
}

type LoadResult struct {
	LoadedSnapshot        any    // as initially loaded from storage
	PreviousHighWatermark uint64 // of the LoadedSnapshot
	NewHighWatermark      uint64 // of the Domain after applying events since previous high watermark
	EventsAppliedCount    uint64 // how many events were applied to the Domain
}

const (
	Latest uint64 = math.MaxUint64
)

type loadConfig struct {
	Logger        logger
	Storage       storage.Storage
	SnapshotID    uint64 // If equal to Latest, load the latest
	Domain        applicator
	EventRegistry struct {
		TypesByName map[string]reflect.Type
		NamesByType map[reflect.Type]string
	}
	LoadedSnapshot any
}

type loadOption func(*loadConfig)

var LoadOptions loading

type loading struct{}

func (loading) Logger(logger logger) loadOption {
	return func(config *loadConfig) { config.Logger = logger }
}

// Storage is used by Load to perform storage operations to retrieve
// the specified snapshot and events since its high watermark.
func (loading) Storage(store storage.Storage) loadOption {
	return func(config *loadConfig) { config.Storage = store }
}

// SnapshotID provides the ID of the specific snapshot to load.
// Providing Latest will load the most recently saved snapshot.
func (loading) SnapshotID(snapshotID uint64) loadOption {
	return func(config *loadConfig) { config.SnapshotID = snapshotID }
}

// Domain provides the object that will have the loaded snapshot applied to it,
// as well as any events (since the snapshot's high watermark) that it can Apply.
func (loading) Domain(domain applicator) loadOption {
	return func(config *loadConfig) { config.Domain = domain }
}

// RegisteredEvents indexes the event types that can be (de)serialized. Providing
// it ALSO opts Load into replaying events since the loaded snapshot's high
// watermark: Load scans the Domain for Apply<Foo>(Foo) methods, resolves each to
// its canonical name via namesByType, and loads those events. Omit it (the
// default) to skip replay entirely and stop at the snapshot.
func (loading) RegisteredEvents(typesByName map[string]reflect.Type, namesByType map[reflect.Type]string) loadOption {
	return func(config *loadConfig) {
		config.EventRegistry.TypesByName = typesByName
		config.EventRegistry.NamesByType = namesByType
	}
}

// LoadedSnapshot is how the caller provides a zero-value pointer to a snapshot,
// which will be populated by Load.
func (loading) LoadedSnapshot(pointer any) loadOption {
	return func(config *loadConfig) { config.LoadedSnapshot = pointer }
}

func (loading) defaults(options ...loadOption) []loadOption {
	var nop loadNop
	return append([]loadOption{
		LoadOptions.Logger(log.Default()),
		LoadOptions.Storage(nop),
		LoadOptions.SnapshotID(Latest),
		LoadOptions.Domain(nop),
		LoadOptions.RegisteredEvents(nil, nil),
		LoadOptions.LoadedSnapshot(struct{}{}),
	}, options...)
}

type loadNop struct{}

func (loadNop) Apply(any) {}

func (loadNop) Exec(context.Context, any) error { return nil }

func load(ctx context.Context, config loadConfig) (result LoadResult, err error) {
	// Load the projection:
	var loadedSnapshotResult storage.LoadedSnapshotResult
	if config.SnapshotID == Latest {
		load := &storage.LoadLatestSnapshot{}
		err = config.Storage.Exec(ctx, load)
		if err != nil {
			return result, err
		}
		loadedSnapshotResult = load.Result
	} else {
		load := &storage.LoadSnapshot{ID: config.SnapshotID}
		err = config.Storage.Exec(ctx, load)
		if err != nil {
			return result, err
		}
		loadedSnapshotResult = load.Result
	}
	if !loadedSnapshotResult.Found {
		return result, errMissingSnapshot
	}

	// Decode/Unmarshal the loaded projection:
	if loadedSnapshotResult.ContentEncoding == "gzip" {
		loadedSnapshotResult.Payload, err = gunzip(loadedSnapshotResult.Payload)
		if err != nil {
			return result, fmt.Errorf("decompress snapshot: %w", err)
		}
	}
	if err = json.Unmarshal(loadedSnapshotResult.Payload, config.LoadedSnapshot); err != nil {
		return result, fmt.Errorf("unmarshal snapshot: %w", err)
	}
	config.Logger.Printf("[INFO] loaded snapshot at high watermark %d", loadedSnapshotResult.HighWatermark)
	result.PreviousHighWatermark = loadedSnapshotResult.HighWatermark
	result.LoadedSnapshot = config.LoadedSnapshot
	config.Domain.Apply(reflect.ValueOf(result.LoadedSnapshot).Elem().Interface()) // de-reference the pointer so the domain will load, not save

	if len(config.EventRegistry.NamesByType) == 0 {
		return result, nil // no registry → caller doesn't want replay
	}
	applied := domainscan.AppliedTypes(config.Domain)
	if len(applied) == 0 {
		return result, nil // registry provided, but the Domain applies no events
	}
	var typeNames []string
	for _, eventType := range applied {
		name, found := config.EventRegistry.NamesByType[eventType]
		if !found {
			return result, fmt.Errorf("%w: %s", errUnregisteredEventType, eventType)
		}
		typeNames = append(typeNames, name)
	}
	// Load and apply events since high watermark:
	loadEvents := &storage.LoadEventsSince{
		HighWatermark: loadedSnapshotResult.HighWatermark,
		Types:         typeNames,
	}
	if err = config.Storage.Exec(ctx, loadEvents); err != nil {
		return result, err
	}
	for _, event := range loadEvents.Result.Events {
		messageType, found := config.EventRegistry.TypesByName[event.Type]
		if !found {
			return result, fmt.Errorf("%w: %q", errUnsupportedMessageType, event.Type)
		}
		pointer := reflect.New(messageType)
		if err := json.Unmarshal(event.Payload, pointer.Interface()); err != nil {
			return result, fmt.Errorf("unmarshal event %q: %w", event.Type, err)
		}
		result.EventsAppliedCount++
		config.Domain.Apply(pointer.Elem().Interface())
	}
	result.NewHighWatermark = loadEvents.Result.NewHighWatermark

	return result, nil
}

func gunzip(compressed []byte) (result []byte, err error) {
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	return io.ReadAll(reader)
}
