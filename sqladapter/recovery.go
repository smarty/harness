package sqladapter

import (
	"context"

	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/internal/storage"
)

// Recovery pages the backlog of Messages WHERE dispatched IS NULL at startup,
// returning each row as a *contracts.Message destined for the Dispatcher
// (publish + mark dispatched). It is a stateful keyset cursor: the pipeline's
// recovery station calls Recover repeatedly from a single goroutine, and each
// call returns the next page (at most limit rows, in id order). On the first
// call it snapshots the backlog's id bounds — MIN/MAX of the undispatched rows
// that predate startup — so rows persisted by live traffic during the recovery
// window fall outside the boundary and are left for the live path. An empty
// page means the backlog is exhausted and recovery is complete. The two SQL
// queries are routed through the storage.DB seam.
type Recovery struct {
	db          contracts.DB
	cursor      uint64 // advances past each successfully returned page; starts at MIN(id)-1 of the backlog
	boundary    uint64 // MAX(id) of the backlog, snapshotted on the first call
	snapshotted bool
}

func NewRecovery(db contracts.DB) *Recovery {
	return &Recovery{db: db}
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

// snapshot records the backlog's id bounds on the first call. An empty backlog
// (op.Found false) leaves cursor and boundary zero so Recover returns the empty
// page without ever issuing a page query. Deriving the lower bound from the
// backlog lets page queries skip the (potentially enormous) prefix of
// already-dispatched rows via a primary-key range; the upper bound stops
// pagination at the backlog that predates startup rather than chasing rows
// written by live traffic.
func (this *Recovery) snapshot(ctx context.Context) error {
	op := new(storage.LoadUndispatchedBounds)
	if err := this.db.Handle(ctx, op); err != nil {
		return err
	}
	if op.Found {
		this.cursor = op.Min - 1 // auto-increment ids start at 1, so no underflow
		this.boundary = op.Max
	}
	this.snapshotted = true
	return nil
}

func (this *Recovery) page(ctx context.Context, limit int) ([]*contracts.Message, error) {
	// NOTE: we're not going to worry about pooling/reusing this operation since
	// recovery is a one-time procedure executed at startup.
	op := &storage.LoadUndispatchedPage{AfterID: this.cursor, ThroughID: this.boundary, Limit: limit}
	if err := this.db.Handle(ctx, op); err != nil {
		return nil, err
	}
	// Advance only after a clean scan: a failed page returns an error above and
	// leaves the cursor untouched, so it is re-served on retry, never skipped.
	if len(op.Messages) > 0 {
		this.cursor = op.Messages[len(op.Messages)-1].ID
	}
	return op.Messages, nil
}
