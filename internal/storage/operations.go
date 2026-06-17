package storage

import (
	"errors"
	"reflect"
	"time"

	"github.com/smarty/harness/v2/contracts"
)

var ErrUnsupportedOperation = errors.New("harness: unsupported storage operation")

type MarkMessagesDispatched struct {
	Messages []*contracts.Message
}

type InsertMessages struct {
	Messages []*contracts.Message
}

// LoadUndispatchedBounds reports the id range of the undispatched backlog.
type LoadUndispatchedBounds struct {
	Found bool   // false when the backlog is empty (both bounds NULL)
	Min   uint64 // populated by the handler
	Max   uint64
}

// LoadUndispatchedPage loads up to Limit undispatched messages in id order
// within the half-open window (AfterID, ThroughID].
type LoadUndispatchedPage struct {
	AfterID   uint64 // exclusive lower bound (the cursor)
	ThroughID uint64 // inclusive upper bound (the frozen boundary)
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
	// LoadLatestSnapshot loads the most recent snapshot row. The target table is
	// configured on the mysql.Mapper (snapshotsTableName), not carried here.
	LoadLatestSnapshot struct {
		Result LoadedSnapshotResult // populated by the handler
	}
	LoadedSnapshotResult struct {
		Found           bool
		HighWatermark   uint64
		Payload         []byte
		ContentType     string
		ContentEncoding string
	}
)

type (
	// LoadEventsSince loads serialized events newer than HighWatermark whose stored
	// type matches one of Events, each resolved to its canonical name via TypeNames.
	LoadEventsSince struct {
		HighWatermark uint64
		Events        []any                   // sample instances; key into TypeNames
		TypeNames     map[reflect.Type]string // reflect.Type → canonical name
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
