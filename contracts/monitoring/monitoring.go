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
