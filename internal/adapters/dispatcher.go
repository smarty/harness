// Package adapters provides a reference implementation of the
// handlers/harness Writer and Dispatcher interfaces, bound to the `Messages`
// MySQL table defined by doc/mysql/schema.sql in this module (columns
// `id`, `dispatched`, `type`, `payload`). Callers running a different schema
// should copy and adapt these types.
package adapters

import (
	"context"
	"errors"

	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/internal/storage"
)

// Dispatcher publishes a batch through its inner dispatcher and then asks the
// contracts.Storage to mark the batch dispatched. It holds a reused buffer for the
// storage operation and is therefore not safe for concurrent use, so a Dispatcher
// must be driven from a single goroutine (as the pipeline does).
type Dispatcher struct {
	inner contracts.Dispatcher
	db    contracts.Storage
	op    *storage.MarkMessagesDispatched
}

// NewDispatcher builds a dispatcher. The inner dispatcher is the caller's
// opportunity to provide an adapter layer to convert between our *contracts.Message
// to their own preferred dispatch type (perhaps a library for RabbitMQ, or Kafka, etc.).
func NewDispatcher(inner contracts.Dispatcher, db contracts.Storage) *Dispatcher {
	return &Dispatcher{
		inner: inner,
		db:    db,
		op:    &storage.MarkMessagesDispatched{},
	}
}

func (this *Dispatcher) Dispatch(ctx context.Context, messages ...*contracts.Message) error {
	if len(messages) == 0 {
		return nil
	}
	// A message with ID==0 was never assigned an identity (a Writer that failed
	// to assign, or a non-adapters Writer). The mark-dispatched UPDATE below
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
	this.op.Messages = messages
	return this.db.Exec(ctx, this.op)
}

var errUnassignedID = errors.New("cannot mark a message dispatched: message has no assigned id (id=0); the Writer must assign positive ids")
