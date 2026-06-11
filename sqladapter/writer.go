package sqladapter

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/smarty/harness/v2/contracts"
)

// Deprecated
type legacyWrite func(context.Context, *sql.Tx, ...any)

type Writer struct {
	handle       *sql.DB
	typeNames    map[reflect.Type]string
	stride       uint64
	legacyWrite  legacyWrite
	legacyValues []any
	args         []any
	statement    *bytes.Buffer
}

// NewWriter builds a Writer that inserts rows into the `Messages` table and
// invokes the supplied legacyWrite function inside the same transaction.
//
// Deprecation warning: the legacyWrite escape hatch is retained for migration from
// other projects and will be removed in a later release; new callers
// should supply a no-op function.
func NewWriter(handle *sql.DB, typeNames map[reflect.Type]string, stride uint64, legacyWrite legacyWrite) *Writer {
	return &Writer{
		handle:       handle,
		typeNames:    typeNames,
		stride:       cmp.Or(stride, 1),
		legacyWrite:  legacyWrite,
		legacyValues: make([]any, 0, 256),
		args:         make([]any, 0, 256),
		statement:    bytes.NewBuffer(make([]byte, 0, 1024*8)),
	}
}

func (this *Writer) Write(ctx context.Context, messages ...*contracts.Message) (err error) {
	if len(messages) == 0 {
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

	if err := this.insertMessages(ctx, tx, messages); err != nil {
		return err
	}

	clear(this.legacyValues)
	this.legacyValues = this.legacyValues[:0]
	for _, message := range messages {
		this.legacyValues = append(this.legacyValues, message.Value)
	}
	this.legacyWrite(ctx, tx, this.legacyValues...)

	return tx.Commit()
}

func (this *Writer) insertMessages(ctx context.Context, tx *sql.Tx, messages []*contracts.Message) error {
	clear(this.args)
	this.args = this.args[:0]
	this.statement.Reset()
	this.statement.WriteString(`INSERT INTO Messages (type, payload) VALUES `)
	for i, message := range messages {
		if message.Type == "" {
			message.Type = this.typeNames[reflect.TypeOf(message.Value)]
		}
		if i > 0 {
			this.statement.WriteString(`,`)
		}
		this.statement.WriteString(`(?, ?)`)
		this.args = append(this.args, message.Type, message.Content.Bytes())
	}
	result, err := tx.ExecContext(ctx, this.statement.String(), this.args...)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	// https://dev.mysql.com/doc/refman/5.6/en/information-functions.html#function_last-insert-id
	// > If you insert multiple rows using a single INSERT statement, LAST_INSERT_ID() returns the value
	// > generated for the first inserted row only.
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
func (this *Writer) assignIDs(messages []*contracts.Message, affected, first int64) error {
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

var (
	errRowsAffected    = errors.New("the number of modified rows was not expected compared to the number of writes performed")
	errIdentityFailure = errors.New("unable to determine the identity of the inserted row(s)")
)
