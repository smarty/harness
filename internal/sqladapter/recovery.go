package sqladapter

import (
	"bytes"
	"context"
	"database/sql"
	"time"

	"github.com/smarty/harness/v2/internal/contracts"
)

type Recovery struct {
	ctx     context.Context
	handle  *sql.DB
	output  chan *contracts.Message
	wait    func(context.Context, time.Duration) error
	monitor contracts.Monitor
}

func NewRecovery(ctx context.Context, handle *sql.DB, output chan *contracts.Message, wait func(context.Context, time.Duration) error, monitor contracts.Monitor) *Recovery {
	return &Recovery{
		ctx:     ctx,
		handle:  handle,
		output:  output,
		wait:    wait,
		monitor: monitor,
	}
}

func (this *Recovery) Listen() {
	for attempt := 1; ; attempt++ {
		err := this.recover()
		if err == nil {
			return
		}
		if this.wait(this.ctx, time.Second) != nil {
			return
		}
		this.monitor.Track(contracts.RecoveryError{Attempts: attempt, Error: err})
	}
}

// recover scans Messages WHERE dispatched IS NULL at startup, wraps each row
// as a *contracts.Message, and feeds them to a channel that will eventually
// lead to the Dispatcher (publish + mark dispatched). Intended to run
// synchronously during initialization, as the harness pipeline starts.
func (this *Recovery) recover() error {
	rows, err := this.handle.QueryContext(this.ctx, `
		SELECT id, type, payload
		  FROM Messages
		 WHERE dispatched IS NULL
		 ORDER BY id;`)
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
		// NOTE: we're not going to worry about pooling/reusing these values since this
		// is a one-time procedure executed at startup.
		this.output <- &contracts.Message{
			ID:      id,
			Type:    typeName,
			Content: bytes.NewBuffer(payload),
			// Hard-coded until the Messages schema gains a content_type column;
			// all payloads written by Writer are JSON so this is correct for now.
			ContentType: "application/json",
		}
	}
	return rows.Err()
}
