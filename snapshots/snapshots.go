// Package snapshots provides domain-agnostic snapshot and event-replay plumbing
// for store-and-forward contexts built on harness/v2. It is the only exported
// surface for snapshot work: it constructs the (internal) storage operations on
// the caller's behalf and dispatches them through a contracts.Storage.
package snapshots

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/internal/storage"
)

// logger receives informational progress messages during initialization.
// A *log.Logger satisfies it.
type logger interface {
	Printf(format string, args ...any)
}

// applicator is the domain being rebuilt: InitializeDomain calls Apply once with
// the decoded snapshot, then once per replayed event in ascending id order.
type applicator interface {
	Apply(message any)
}

// DomainInitializationReport summarizes the outcome of InitializeDomain.
type DomainInitializationReport struct {
	PreviousHighWatermark uint64
	NewHighWatermark      uint64
	EventsAppliedCount    uint64
}

// PopulateSnapshot decompresses (if gzip) and unmarshals a snapshot payload into S.
func PopulateSnapshot[S any](logger logger, payload []byte, contentEncoding string, highWatermark uint64) (snapshot S, err error) {
	if contentEncoding == "gzip" {
		payload, err = gunzip(payload)
		if err != nil {
			return snapshot, fmt.Errorf("decompress snapshot: %w", err)
		}
	}
	if err = json.Unmarshal(payload, &snapshot); err != nil {
		return snapshot, fmt.Errorf("unmarshal snapshot: %w", err)
	}
	logger.Printf("[INFO] loaded snapshot at high watermark %d", highWatermark)
	return snapshot, nil
}

// InitializeDomain loads the latest snapshot (from the snapshot table configured
// on the mysql.Mapper), applies it, then loads and applies every event newer than
// the snapshot's high watermark.
func InitializeDomain[S any](
	ctx context.Context,
	logger logger,
	db contracts.Storage,
	messageTypes map[string]reflect.Type,
	typeNames map[reflect.Type]string,
	domain applicator,
	events ...any,
) (
	result DomainInitializationReport,
	err error,
) {
	latest := &storage.LoadLatestSnapshot{}
	if err := db.Exec(ctx, latest); err != nil {
		return result, err
	}
	if !latest.Result.Found {
		return result, errMissingSnapshot
	}
	snapshot, err := PopulateSnapshot[S](logger, latest.Result.Payload, latest.Result.ContentEncoding, latest.Result.HighWatermark)
	if err != nil {
		return result, err
	}
	result.PreviousHighWatermark = latest.Result.HighWatermark
	result.NewHighWatermark = latest.Result.HighWatermark
	domain.Apply(snapshot)

	decoded, newHighWatermark, err := LoadEventsSince(ctx, db, latest.Result.HighWatermark, messageTypes, typeNames, events...)
	if err != nil {
		return result, err
	}
	for _, event := range decoded {
		domain.Apply(event)
	}
	result.EventsAppliedCount = uint64(len(decoded))
	if newHighWatermark > result.NewHighWatermark {
		result.NewHighWatermark = newHighWatermark
	}
	logger.Printf("[INFO] initialized domain: applied %d event(s) through high watermark %d",
		result.EventsAppliedCount, result.NewHighWatermark)
	return result, nil
}

// LoadEventsSince loads, decodes, and returns events newer than highWatermark, for
// callers (snapshot-inspect, snapshot-to-sqlite) that replay against a chosen snapshot.
func LoadEventsSince(
	ctx context.Context,
	db contracts.Storage,
	highWatermark uint64,
	messageTypes map[string]reflect.Type,
	typeNames map[reflect.Type]string,
	events ...any,
) (
	decoded []any,
	newHighWatermark uint64,
	err error,
) {
	operation := &storage.LoadEventsSince{
		HighWatermark: highWatermark,
		Events:        events,
		TypeNames:     typeNames,
	}
	if err = db.Exec(ctx, operation); err != nil {
		return nil, 0, err
	}
	decoded, err = decodeEvents(operation.Result.Events, messageTypes)
	if err != nil {
		return nil, 0, err
	}
	return decoded, operation.Result.NewHighWatermark, nil
}

func decodeEvents(events []storage.Event, messageTypes map[string]reflect.Type) (decoded []any, err error) {
	for _, event := range events {
		messageType, found := messageTypes[event.Type]
		if !found {
			return nil, fmt.Errorf("%w: %q", errUnsupportedMessageType, event.Type)
		}
		pointer := reflect.New(messageType)
		if err := json.Unmarshal(event.Payload, pointer.Interface()); err != nil {
			return nil, fmt.Errorf("unmarshal event %q: %w", event.Type, err)
		}
		decoded = append(decoded, pointer.Elem().Interface())
	}
	return decoded, nil
}

func gunzip(compressed []byte) (result []byte, err error) {
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	return io.ReadAll(reader)
}

var (
	errMissingSnapshot        = errors.New("snapshots: no snapshot found")
	errUnsupportedMessageType = errors.New("snapshots: unsupported message type")
)
