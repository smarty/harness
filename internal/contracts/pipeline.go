package contracts

import "net/http"

type Pipeline struct {
	// SheddingHTTPWrapper is meant to wrap around any http.Handler that calls SheddingEntrypoint.
	// It responds with HTTP 503 in the event that the handler is backed up beyond the configured ShedThreshold.
	SheddingHTTPWrapper func(http.Handler) http.Handler

	// SheddingEntrypoint is a Handler that is meant to be guarded by an admitter (such as SheddingHTTPWrapper).
	SheddingEntrypoint Handler

	// BlockingEntrypoint is a Handler that will block until the results of the provided work have been durably stored.
	BlockingEntrypoint Handler

	// Listeners contains each phase of the harness pipeline (serialization, persistence, broadcast, etc.).
	// Each listener should be invoked on a separate goroutine by a component like github.com/smarty/dominoes.
	Listeners []Listener
}
