package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/smarty/harness/v2/internal/generic"
	"github.com/smarty/harness/v2/internal/storage"
)

// Mapper leverages a pool of re-usable statement buffers and is safe for concurrent use.
type Mapper struct {
	handle     *sql.DB
	statements *generic.PoolT[*statement]
}

func NewMapper(handle *sql.DB) *Mapper {
	return &Mapper{
		handle:     handle,
		statements: generic.NewPoolT(newStatement),
	}
}

func (this *Mapper) Handle(ctx context.Context, operation any) error {
	switch op := operation.(type) {
	case *storage.MarkMessagesDispatched:
		return this.markMessagesDispatched(ctx, op)
	default:
		return storage.ErrUnsupportedOperation
	}
}

func (this *Mapper) markMessagesDispatched(ctx context.Context, operation *storage.MarkMessagesDispatched) error {
	statement := this.statements.Get()
	defer this.statements.Put(statement)
	statement.reset()

	statement.text.WriteString(`UPDATE Messages SET dispatched = NOW(3) WHERE dispatched IS NULL AND id IN (`)
	for i, message := range operation.Messages {
		if i > 0 {
			statement.text.WriteString(`,`)
		}
		statement.text.WriteString(`?`)
		statement.args = append(statement.args, message.ID)
	}
	statement.text.WriteString(`)`)
	if _, err := this.handle.ExecContext(ctx, statement.text.String(), statement.args...); err != nil {
		return fmt.Errorf("mark dispatched: %w", err)
	}
	return nil
}
