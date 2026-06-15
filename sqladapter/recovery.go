package sqladapter

import (
	"bytes"
	"context"
	"database/sql"

	"github.com/smarty/harness/v2/contracts"
)

// Recovery pages the backlog of Messages WHERE dispatched IS NULL at startup,
// returning each row as a *contracts.Message destined for the Dispatcher
// (publish + mark dispatched). It is a stateful keyset cursor: the pipeline's
// recovery station calls Recover repeatedly from a single goroutine, and each
// call returns the next page (at most limit rows, in id order). On the first
// call it snapshots the backlog's id bounds — MIN/MAX of the undispatched rows
// that predate startup — so rows persisted by live traffic during the recovery
// window fall outside the boundary and are left for the live path. An empty
// page means the backlog is exhausted and recovery is complete.
type Recovery struct {
	handle      *sql.DB
	cursor      uint64 // advances past each successfully returned page; starts at MIN(id)-1 of the backlog
	boundary    uint64 // MAX(id) of the backlog, snapshotted on the first call
	snapshotted bool
}

func NewRecovery(handle *sql.DB) *Recovery {
	return &Recovery{handle: handle}
}

func (this *Recovery) Recover(ctx context.Context, limit int) ([]*contracts.Message, error) {
	if !this.snapshotted {
		if err := this.snapshot(ctx); err != nil {
			return nil, err
		}
	}
	if this.cursor >= this.boundary {
		return nil, nil
	}
	return this.page(ctx, limit)
}

// snapshot records the backlog's id bounds on the first call. Both bounds NULL
// means there is no backlog: cursor and boundary stay zero so Recover returns
// the empty page without ever issuing a page query. Deriving the lower bound
// from the backlog lets page queries skip the (potentially enormous) prefix of
// already-dispatched rows via a primary-key range; the upper bound stops
// pagination at the backlog that predates startup rather than chasing rows
// written by live traffic.
func (this *Recovery) snapshot(ctx context.Context) error {
	var lo, hi sql.NullInt64
	row := this.handle.QueryRowContext(ctx, `
		SELECT MIN(id), MAX(id)
		  FROM Messages
		 WHERE dispatched IS NULL;`)
	if err := row.Scan(&lo, &hi); err != nil {
		return err
	}
	if lo.Valid && hi.Valid {
		this.cursor = uint64(lo.Int64) - 1 // auto-increment ids start at 1, so no underflow
		this.boundary = uint64(hi.Int64)
	}
	this.snapshotted = true
	return nil
}

func (this *Recovery) page(ctx context.Context, limit int) (results []*contracts.Message, err error) {
	rows, err := this.handle.QueryContext(ctx, `
		SELECT id, type, payload
		  FROM Messages
		 WHERE dispatched IS NULL AND id > ? AND id <= ?
		 ORDER BY id
		 LIMIT ?;`, this.cursor, this.boundary, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var lastID uint64
	for rows.Next() {
		var (
			id       uint64
			typeName string
			payload  []byte
		)
		if err := rows.Scan(&id, &typeName, &payload); err != nil {
			return nil, err
		}
		// NOTE: we're not going to worry about pooling/reusing these values since this
		// is a one-time procedure executed at startup.
		results = append(results, &contracts.Message{
			ID:      id,
			Type:    typeName,
			Content: bytes.NewBuffer(payload),
			// Hard-coded until the Messages schema gains a content_type column;
			// all payloads written by Writer are JSON so this is correct for now.
			ContentType: "application/json",
		})
		lastID = id
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	// Advance only after a clean scan (including rows.Err): a failed page is
	// re-served on retry, never skipped.
	if len(results) > 0 {
		this.cursor = lastID
	}
	return results, nil
}
