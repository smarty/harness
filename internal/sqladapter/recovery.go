package sqladapter

import (
	"bytes"
	"context"
	"database/sql"

	"github.com/smarty/harness/v2/contracts"
)

// Recovery scans Messages WHERE dispatched IS NULL at startup and returns each
// row as a *contracts.Message, destined for the Dispatcher (publish + mark
// dispatched). Invoked by the pipeline's recovery station as the harness starts.
type Recovery struct {
	handle *sql.DB
}

func NewRecovery(handle *sql.DB) *Recovery {
	return &Recovery{handle: handle}
}

func (this *Recovery) Recover(ctx context.Context) (results []*contracts.Message, err error) {
	rows, err := this.handle.QueryContext(ctx, `
		SELECT id, type, payload
		  FROM Messages
		 WHERE dispatched IS NULL
		 ORDER BY id;`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			id       uint64
			typeName string
			payload  []byte
		)
		if err := rows.Scan(&id, &typeName, &payload); err != nil {
			return nil, err
		}
		// NOTE: we're not going to worry about pooling/reusing these values since this
		// is a one-time procedure executed at startup.
		results = append(results, &contracts.Message{
			ID:      id,
			Type:    typeName,
			Content: bytes.NewBuffer(payload),
			// Hard-coded until the Messages schema gains a content_type column;
			// all payloads written by Writer are JSON so this is correct for now.
			ContentType: "application/json",
		})
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return results, nil
}
