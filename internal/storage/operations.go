package storage

import (
	"context"
	"errors"
	"time"

	"github.com/smarty/harness/v2/contracts"
)

var ErrUnsupportedOperation = errors.New("harness: unsupported storage operation")

// Storage executes one of the operation types defined in this package against a
// concrete datastore, populating result fields on the operation by pointer. It
// is the module-private seam between the pipeline/snapshots and storage: the
// operation types live in this internal package, so the interface is
// intentionally not implementable by external callers. The only implementation
// is storage/mysql.Mapper. Callers wire it via harness.Options.Storage(...),
// snapshots.LoadOptions.Storage(...), and snapshots.SaveOptions.Storage(...) by
// passing a concrete *mysql.Mapper; they never name this type.
type Storage interface {
	Exec(ctx context.Context, operation any) error
}

type MarkMessagesDispatched struct {
	Messages []*contracts.Message
}

type InsertMessages struct {
	Messages []*contracts.Message
}

// LoadUndispatchedBounds reports the id range of the undispatched backlog,
// scoped to the given stored type names (an empty Types means unscoped: all
// types are considered).
type LoadUndispatchedBounds struct {
	Types []string // sorted stored type names to scope to; empty = unscoped
	Found bool     // false when the backlog is empty (both bounds NULL)
	Min   uint64   // populated by the handler
	Max   uint64
}

// LoadUndispatchedPage loads up to Limit undispatched messages in id order
// within the half-open window (AfterID, ThroughID], scoped to the given stored
// type names (an empty Types means unscoped: all types are considered).
type LoadUndispatchedPage struct {
	Types     []string // sorted stored type names to scope to; empty = unscoped
	AfterID   uint64   // exclusive lower bound (the cursor)
	ThroughID uint64   // inclusive upper bound (the frozen boundary)
	Limit     int
	Messages  []*contracts.Message // populated by the handler
}

// SaveSnapshot persists one domain snapshot row. The target table is configured
// on the mysql.Mapper (snapshotsTableName), not carried on the operation.
type SaveSnapshot struct {
	Timestamp       time.Time
	HighWatermark   uint64
	Payload         []byte
	ContentType     string
	ContentEncoding string
}

type (
	// LoadSnapshot loads the snapshot row with the matching ID. The target table
	// is configured on the mysql.Mapper (snapshotsTableName), not carried here.
	LoadSnapshot struct {
		ID     uint64
		Result LoadedSnapshotResult // populated by the handler
	}
	// LoadLatestSnapshot loads the most recent snapshot row. The target table is
	// configured on the mysql.Mapper (snapshotsTableName), not carried here.
	LoadLatestSnapshot struct {
		Result LoadedSnapshotResult // populated by the handler
	}
	LoadedSnapshotResult struct {
		Found           bool
		SnapshotID      uint64
		HighWatermark   uint64
		Payload         []byte
		ContentType     string
		ContentEncoding string
	}
)

type (
	// LoadEventsSince loads serialized events newer than HighWatermark whose stored
	// type matches one of Types (canonical stored type names).
	LoadEventsSince struct {
		HighWatermark uint64
		Types         map[string]struct{} // set of canonical stored type names to query for
		Result        struct {
			NewHighWatermark uint64
			Events           []Event
		} // populated by the handler
	}
	Event struct {
		Type    string
		Payload []byte
	}
)
