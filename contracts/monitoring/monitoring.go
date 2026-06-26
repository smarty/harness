// Package monitoring contains observations emitted by the pipeline.
package monitoring

import "errors"

type (
	BatchInFlight      struct{}
	BatchComplete      struct{}
	BatchAbandoned     struct{}
	LoadShed           struct{}
	CallerDeparted     struct{}
	SerializationError struct {
		Value any
		Error error
	}
	PersistenceError struct {
		Attempt int
		Error   error
	}
	PersistenceAbandoned struct{ Attempts int }
	BroadcastError       struct {
		Attempt int
		Error   error
	}
	BroadcastAbandoned struct{ Attempts int }
	RecoveryError      struct {
		Attempt int
		Error   error
	}
	RecoveryAbandoned struct{ Attempts int }
	RecoveryComplete  struct{ Count int }

	// Throughput counters: each carries the count for one processed unit of work.
	// ResultsPersisted and ResultsDispatched are not expected to match exactly:
	// persistence drops a unit on shutdown-abandonment (so it is never dispatched),
	// while broadcast always forwards already-durable work and additionally re-emits
	// the recovered startup backlog — so dispatched legitimately exceeds persisted.
	InstructionsHandled struct{ Count int } // execution: commands run per batch
	ResultsPersisted    struct{ Count int } // persistence: messages durably written
	ResultsDispatched   struct{ Count int } // broadcast: messages published downstream
)

var (
	// ErrBatchAbandoned is the panic value raised by a blocked entrypoint caller
	// whose work was abandoned (context cancelled before a durable write); the
	// panic guarantees message brokers never acknowledge unstored work.
	ErrBatchAbandoned = errors.New("harness: batch abandoned before durable write; " +
		"panicking so the message broker never acknowledges and will redeliver")

	ErrSerialization = errors.New("harness: serialization error")
	ErrPersistence   = errors.New("harness: persistence error")
	ErrBroadcast     = errors.New("harness: broadcast error")
	ErrRecovery      = errors.New("harness: recovery error")
)
