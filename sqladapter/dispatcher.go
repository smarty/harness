// Package sqladapter provides a reference implementation of the
// handlers/harness Writer and Dispatcher interfaces, bound to the `Messages`
// MySQL table defined by doc/mysql/schema.sql in this module (columns
// `id`, `dispatched`, `type`, `payload`). Callers running a different schema
// should copy and adapt these types.
package sqladapter

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/internal/generic"
)

// Steady-state capacities retained for the Dispatcher's reused buffers; a
// pathologically large batch has its oversized backing arrays discarded on the
// next call rather than pinned for the life of the process (see generic.Reclaim).
const (
	dispatcherArgsCapacity      = 512
	dispatcherStatementCapacity = 1024 * 8
)

// Dispatcher reuses instance-level statement and argument buffers across calls
// and is therefore not safe for concurrent use; it must be driven from a single
// goroutine (as the pipeline does). Sharing one Dispatcher across goroutines
// yields interleaved SQL.
type Dispatcher struct {
	inner     contracts.Dispatcher
	handle    *sql.DB
	args      []any
	statement *bytes.Buffer
}

// NewDispatcher builds a dispatcher. The inner dispatcher is the caller's
// opportunity to provide an adapter layer to convert between our *contracts.Message
// to their own preferred dispatch type (perhaps a library for RabbitMQ, or Kafka, etc.).
func NewDispatcher(inner contracts.Dispatcher, handle *sql.DB) *Dispatcher {
	return &Dispatcher{
		inner:     inner,
		handle:    handle,
		args:      make([]any, 0, dispatcherArgsCapacity),
		statement: bytes.NewBuffer(make([]byte, 0, dispatcherStatementCapacity)),
	}
}

func (this *Dispatcher) Dispatch(ctx context.Context, messages ...*contracts.Message) error {
	if len(messages) == 0 {
		return nil
	}
	// A message with ID==0 was never assigned an identity (a Writer that failed
	// to assign, or a non-sqladapter Writer). The mark-dispatched UPDATE below
	// keys on id, so such a row can never be matched: it would be published here
	// and then republished by recovery on every restart, forever. Reject the
	// batch before publishing — symmetric to the Writer's first<=0 identity
	// guard — so the un-markable message is never dispatched in the first place.
	for _, message := range messages {
		if message.ID == 0 {
			return errUnassignedID
		}
	}
	if err := this.inner.Dispatch(ctx, messages...); err != nil {
		return err
	}

	this.args = generic.Reclaim(this.args, dispatcherArgsCapacity)
	this.statement = generic.ReclaimBuffer(this.statement, dispatcherStatementCapacity)
	this.statement.WriteString(`UPDATE Messages SET dispatched = NOW(3) WHERE dispatched IS NULL AND id IN (`)
	for i, message := range messages {
		if i > 0 {
			this.statement.WriteString(`,`)
		}
		this.statement.WriteString(`?`)
		this.args = append(this.args, message.ID)
	}
	this.statement.WriteString(`)`)
	// Rows-affected is intentionally not asserted against len(messages): the
	// `dispatched IS NULL` guard makes redelivery idempotent, so after a crash
	// between publish and mark, recovery republishes and this UPDATE legitimately
	// matches fewer rows (or none) because they are already marked. The only
	// unrecoverable case — an id that can never match — is the ID==0 guard above.
	if _, err := this.handle.ExecContext(ctx, this.statement.String(), this.args...); err != nil {
		return fmt.Errorf("mark dispatched: %w", err)
	}
	return nil
}

var errUnassignedID = errors.New("cannot mark a message dispatched: message has no assigned id (id=0); the Writer must assign positive ids")
