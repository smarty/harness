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
	"fmt"

	"github.com/smarty/harness/v2/contracts"
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
		args:      make([]any, 0, 512),
		statement: bytes.NewBuffer(make([]byte, 0, 1024*8)),
	}
}

func (this *Dispatcher) Dispatch(ctx context.Context, messages ...*contracts.Message) error {
	if len(messages) == 0 {
		return nil
	}
	if err := this.inner.Dispatch(ctx, messages...); err != nil {
		return err
	}

	clear(this.args)
	this.args = this.args[:0]
	this.statement.Reset()
	this.statement.WriteString(`UPDATE Messages SET dispatched = NOW(3) WHERE dispatched IS NULL AND id IN (`)
	for i, message := range messages {
		if i > 0 {
			this.statement.WriteString(`,`)
		}
		this.statement.WriteString(`?`)
		this.args = append(this.args, message.ID)
	}
	this.statement.WriteString(`)`)
	if _, err := this.handle.ExecContext(ctx, this.statement.String(), this.args...); err != nil {
		return fmt.Errorf("mark dispatched: %w", err)
	}
	return nil
}
