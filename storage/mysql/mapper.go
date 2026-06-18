package mysql

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"slices"

	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/internal/generic"
	"github.com/smarty/harness/v2/internal/storage"
)

// Mapper leverages a pool of re-usable statement buffers and is safe for concurrent use.
type Mapper struct {
	handle         *sql.DB
	stride         uint64
	snapshotsTable string
	messagesTable  string
	statements     *generic.PoolT[*statement]
	legacyWrite    func(context.Context, *sql.Tx, ...any) // Deprecated; never nil — no-op default
	legacyWrites   []any                                  // Deprecated
}

func NewMapper(handle *sql.DB, stride uint64, snapshotsTableName, messagesTableName string) *Mapper {
	return &Mapper{
		handle:         handle,
		stride:         cmp.Or(stride, 1),
		snapshotsTable: snapshotsTableName,
		messagesTable:  messagesTableName,
		statements:     generic.NewPoolT(newStatement),
		legacyWrite:    func(context.Context, *sql.Tx, ...any) {}, // no-op until overridden
		legacyWrites:   make([]any, 0, legacyWritesBufferCapacity),
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

func (this *Mapper) Exec(ctx context.Context, operation any) error {
	switch op := operation.(type) {
	case *storage.MarkMessagesDispatched:
		return this.markMessagesDispatched(ctx, op)
	case *storage.InsertMessages:
		return this.insertMessages(ctx, op)
	case *storage.LoadUndispatchedBounds:
		return this.loadUndispatchedBounds(ctx, op)
	case *storage.LoadUndispatchedPage:
		return this.loadUndispatchedPage(ctx, op)
	case *storage.SaveSnapshot:
		return this.saveSnapshot(ctx, op)
	case *storage.LoadLatestSnapshot:
		return this.loadLatestSnapshot(ctx, op)
	case *storage.LoadSnapshot:
		return this.loadSnapshot(ctx, op)
	case *storage.LoadEventsSince:
		return this.loadEventsSince(ctx, op)
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
	table, err := quoteTableName(this.messagesTable)
	if err != nil {
		return err
	}
	statement := this.statements.Get()
	defer this.statements.Put(statement)
	statement.reset()
	statement.text.WriteString(fmt.Sprintf(`INSERT INTO %s (type, payload) VALUES `, table))
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
	table, err := quoteTableName(this.messagesTable)
	if err != nil {
		return err
	}
	statement := this.statements.Get()
	defer this.statements.Put(statement)
	statement.reset()

	statement.text.WriteString(fmt.Sprintf(`
		UPDATE %s
		   SET dispatched = NOW(3)
		 WHERE dispatched IS NULL
		   AND id IN (`, table))
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

func (this *Mapper) loadUndispatchedBounds(ctx context.Context, operation *storage.LoadUndispatchedBounds) error {
	table, err := quoteTableName(this.messagesTable)
	if err != nil {
		return err
	}
	var lo, hi sql.NullInt64
	row := this.handle.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT MIN(id), MAX(id)
		  FROM %s
		 WHERE dispatched IS NULL`, table))
	if err := row.Scan(&lo, &hi); err != nil {
		return err
	}
	if lo.Valid && hi.Valid {
		operation.Found, operation.Min, operation.Max = true, uint64(lo.Int64), uint64(hi.Int64)
	}
	return nil
}

func (this *Mapper) loadUndispatchedPage(ctx context.Context, operation *storage.LoadUndispatchedPage) error {
	table, err := quoteTableName(this.messagesTable)
	if err != nil {
		return err
	}
	rows, err := this.handle.QueryContext(ctx, fmt.Sprintf(`
		SELECT id, type, payload
		  FROM %s
		 WHERE dispatched IS NULL
		   AND id > ?
		   AND id <= ?
		 ORDER BY id
		 LIMIT ?`, table), operation.AfterID, operation.ThroughID, operation.Limit)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			id       uint64
			typeName string
			payload  []byte
		)
		if err := rows.Scan(&id, &typeName, &payload); err != nil {
			return err
		}
		operation.Messages = append(operation.Messages, &contracts.Message{
			ID:      id,
			Type:    typeName,
			Content: bytes.NewBuffer(payload),
			// Hard-coded until the Messages schema gains a content_type column;
			// all payloads written by Writer are JSON so this is correct for now.
			ContentType: "application/json",
		})
	}
	// A mid-scan error returns non-nil; Recovery will not advance its cursor.
	return rows.Err()
}

func (this *Mapper) saveSnapshot(ctx context.Context, operation *storage.SaveSnapshot) error {
	table, err := quoteTableName(this.snapshotsTable)
	if err != nil {
		return err
	}
	query := fmt.Sprintf(
		`INSERT INTO %s (created, high_watermark, payload, content_type, content_encoding) VALUES (?, ?, ?, ?, ?)`,
		table)
	if _, err := this.handle.ExecContext(ctx, query,
		operation.Timestamp, operation.HighWatermark, operation.Payload,
		operation.ContentType, operation.ContentEncoding); err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}
	return nil
}

func (this *Mapper) loadLatestSnapshot(ctx context.Context, operation *storage.LoadLatestSnapshot) error {
	table, err := quoteTableName(this.snapshotsTable)
	if err != nil {
		return err
	}
	query := fmt.Sprintf(`
		SELECT high_watermark, payload, content_type, content_encoding
		  FROM %s
		 ORDER BY id DESC
		 LIMIT 1`, table)
	var result storage.LoadedSnapshotResult
	row := this.handle.QueryRowContext(ctx, query)
	err = row.Scan(&result.HighWatermark, &result.Payload, &result.ContentType, &result.ContentEncoding)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	result.Found = true
	operation.Result = result
	return nil
}

func (this *Mapper) loadSnapshot(ctx context.Context, operation *storage.LoadSnapshot) error {
	table, err := quoteTableName(this.snapshotsTable)
	if err != nil {
		return err
	}
	query := fmt.Sprintf(`
		SELECT high_watermark, payload, content_type, content_encoding
		  FROM %s
		 WHERE id = ?`, table)
	var result storage.LoadedSnapshotResult
	row := this.handle.QueryRowContext(ctx, query, operation.ID)
	err = row.Scan(&result.HighWatermark, &result.Payload, &result.ContentType, &result.ContentEncoding)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	result.Found = true
	operation.Result = result
	return nil
}

func (this *Mapper) loadEventsSince(ctx context.Context, op *storage.LoadEventsSince) error {
	table, err := quoteTableName(this.messagesTable)
	if err != nil {
		return err
	}
	statement := this.statements.Get()
	defer this.statements.Put(statement)
	statement.reset()

	statement.text.WriteString(fmt.Sprintf(`
		SELECT id, type, payload
		  FROM %s
		 WHERE type IN (`, table),
	)

	for e, name := range op.Types {
		if slices.Contains(statement.args, any(name)) {
			continue
		}
		statement.args = append(statement.args, name)
		statement.text.WriteString(`?`)
		if e < len(op.Types)-1 {
			statement.text.WriteString(`,`)
		}
	}
	if len(statement.args) == 0 {
		return errors.New("no valid event types provided")
	}
	statement.args = append(statement.args, op.HighWatermark)
	statement.text.WriteString(`) AND id > ? ORDER BY id ASC`)

	rows, err := this.handle.QueryContext(ctx, statement.text.String(), statement.args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var highWatermark uint64
		var event storage.Event
		err = rows.Scan(&highWatermark, &event.Type, &event.Payload)
		if err != nil {
			return err
		}
		op.Result.NewHighWatermark = highWatermark
		op.Result.Events = append(op.Result.Events, event)
	}
	return nil
}

// quoteTableName guards against SQL injection through interpolated table names:
// table names cannot be bound as ? placeholders, so only a strict identifier is
// accepted and it is wrapped in backticks before reaching the query string.
func quoteTableName(name string) (string, error) {
	if !tableNamePattern.MatchString(name) {
		return "", fmt.Errorf("%w: %q", errInvalidTableName, name)
	}
	return fmt.Sprintf("`%s`", name), nil
}

var (
	errRowsAffected     = errors.New("the number of modified rows was not expected compared to the number of writes performed")
	errIdentityFailure  = errors.New("unable to determine the identity of the inserted row(s)")
	errInvalidTableName = errors.New("table name is not a valid SQL identifier")

	tableNamePattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)
)

const (
	legacyWritesBufferCapacity = 64 // Deprecated
)
