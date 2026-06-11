// Package sqladapter provides a reference implementation of the
// handlers/harness Writer and Dispatcher interfaces, bound to the `Messages`
// MySQL table defined by doc/mysql/schema.sql in this module (columns
// `id`, `dispatched`, `type`, `payload`). Callers running a different schema
// should copy and adapt these types.
package sqladapter

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/smarty/harness/v2/contracts"
)

type Dispatcher struct {
	inner  contracts.Dispatcher
	handle *sql.DB
}

func NewDispatcher(inner contracts.Dispatcher, handle *sql.DB) *Dispatcher {
	return &Dispatcher{
		inner:  inner,
		handle: handle,
	}
}

func (this *Dispatcher) Dispatch(ctx context.Context, messages ...*contracts.Message) error {
	if len(messages) == 0 {
		return nil
	}
	if err := this.inner.Dispatch(ctx, messages...); err != nil {
		return err
	}

	var statement strings.Builder // TODO: reuse statement builder
	statement.WriteString(`UPDATE Messages SET dispatched = NOW(3) WHERE dispatched IS NULL AND id IN (`)
	args := make([]any, 0, len(messages)) // TODO: reuse args buffer
	for i, message := range messages {
		if i > 0 {
			statement.WriteString(`,`)
		}
		statement.WriteString(`?`)
		args = append(args, message.ID)
	}
	statement.WriteString(`)`)
	if _, err := this.handle.ExecContext(ctx, statement.String(), args...); err != nil {
		return fmt.Errorf("mark dispatched: %w", err)
	}
	return nil
}
