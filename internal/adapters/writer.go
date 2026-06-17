package adapters

import (
	"context"

	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/internal/storage"
)

// Writer persists a batch of messages by handing a storage.InsertMessages
// operation to the contracts.Storage. It holds a reusable buffer for the storage
// operation so is not safe for concurrent use. A Writer must be driven
// from a single goroutine (as the pipeline does).
type Writer struct {
	db contracts.Storage
	op *storage.InsertMessages
}

// NewWriter builds a Writer that inserts rows into the `Messages` table via the
// supplied contracts.Storage.
func NewWriter(db contracts.Storage) *Writer {
	return &Writer{
		db: db,
		op: new(storage.InsertMessages),
	}
}

func (this *Writer) Write(ctx context.Context, messages ...*contracts.Message) error {
	if len(messages) == 0 {
		return nil
	}
	this.op.Messages = messages
	return this.db.Exec(ctx, this.op)
}
