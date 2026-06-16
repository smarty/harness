package mysql

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/internal/generic"
	"github.com/smarty/harness/v2/internal/storage"
)

// Mapper leverages a pool of re-usable statement buffers and is safe for concurrent use.
type Mapper struct {
	handle       *sql.DB
	stride       uint64
	statements   *generic.PoolT[*statement]
	legacyWrite  func(context.Context, *sql.Tx, ...any) // Deprecated; never nil — no-op default
	legacyWrites []any                                  // Deprecated
}

func NewMapper(handle *sql.DB, stride uint64) *Mapper {
	return &Mapper{
		handle:       handle,
		stride:       cmp.Or(stride, 1),
		statements:   generic.NewPoolT(newStatement),
		legacyWrite:  func(context.Context, *sql.Tx, ...any) {}, // no-op until overridden
		legacyWrites: make([]any, 0, legacyWritesBufferCapacity),
	}
}

// Deprecated
//
// WithLegacyWrite registers the transitional hook run inside the same transaction
// as the message INSERT, and returns the Mapper for chaining.
//
// Deprecation warning: this escape hatch is retained for migration from other
// projects and will be removed in a later release; new callers should omit it.
func (this *Mapper) WithLegacyWrite(legacyWrite func(context.Context, *sql.Tx, ...any)) *Mapper {
	this.legacyWrite = legacyWrite
	return this
}

func (this *Mapper) Handle(ctx context.Context, operation any) error {
	switch op := operation.(type) {
	case *storage.MarkMessagesDispatched:
		return this.markMessagesDispatched(ctx, op)
	case *storage.InsertMessages:
		return this.insertMessages(ctx, op)
	default:
		return storage.ErrUnsupportedOperation
	}
}

func (this *Mapper) insertMessages(ctx context.Context, operation *storage.InsertMessages) (err error) {
	if len(operation.Messages) == 0 {
		return nil
	}
	tx, err := this.handle.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = tx.Rollback()
			err = fmt.Errorf("panic during write: %v", recovered)
		} else if err != nil {
			_ = tx.Rollback()
		}
	}()
	if err := this.insert(ctx, tx, operation.Messages); err != nil {
		return err
	}
	this.performLegacyWrite(ctx, tx, operation)
	return tx.Commit()
}

// Deprecated
func (this *Mapper) performLegacyWrite(ctx context.Context, tx *sql.Tx, operation *storage.InsertMessages) {
	this.legacyWrites = generic.Reclaim(this.legacyWrites, legacyWritesBufferCapacity)
	for _, message := range operation.Messages {
		this.legacyWrites = append(this.legacyWrites, message.Value)
	}
	// The transaction exists solely so this deprecated hook commits atomically
	// with the INSERT; it defaults to a no-op, so it is always safe to call.
	this.legacyWrite(ctx, tx, this.legacyWrites...)
}
func (this *Mapper) insert(ctx context.Context, tx *sql.Tx, messages []*contracts.Message) error {
	statement := this.statements.Get()
	defer this.statements.Put(statement)
	statement.reset()
	statement.text.WriteString(`INSERT INTO Messages (type, payload) VALUES `)
	for i, message := range messages {
		if i > 0 {
			statement.text.WriteString(`,`)
		}
		statement.text.WriteString(`(?, ?)`)
		statement.args = append(statement.args, message.Type, message.Content.Bytes())
	}
	result, err := tx.ExecContext(ctx, statement.text.String(), statement.args...)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	// https://dev.mysql.com/doc/refman/8.4/en/information-functions.html#function_last-insert-id
	// A single multi-row INSERT reports the identity of the first inserted row only.
	first, err := result.LastInsertId()
	if err != nil {
		return err
	}
	return this.assignIDs(messages, affected, first)
}

// assignIDs validates the row count and starting identity reported by the
// INSERT, then derives each message's ID from the first auto-increment value.
// The derivation relies on a single multi-row "simple insert" producing
// consecutive auto-increment values spaced by stride, which holds even under
// innodb_autoinc_lock_mode = 2 so long as no concurrent "bulk inserts" target
// the Messages table and stride matches the server's auto_increment_increment.
// https://dev.mysql.com/doc/refman/8.4/en/innodb-auto-increment-handling.html
func (this *Mapper) assignIDs(messages []*contracts.Message, affected, first int64) error {
	if affected != int64(len(messages)) {
		return errRowsAffected
	}
	if first <= 0 {
		return errIdentityFailure
	}
	for i, message := range messages {
		message.ID = uint64(first) + uint64(i)*this.stride
	}
	return nil
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

var (
	errRowsAffected    = errors.New("the number of modified rows was not expected compared to the number of writes performed")
	errIdentityFailure = errors.New("unable to determine the identity of the inserted row(s)")
)

const (
	legacyWritesBufferCapacity = 64 // Deprecated
)
