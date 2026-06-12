// Package monitoring contains observations emitted by the pipeline.
package monitoring

import "errors"

type (
	BatchInFlight      struct{}
	BatchComplete      struct{}
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
)

var (
	ErrSerialization = errors.New("harness: serialization error")
	ErrPersistence   = errors.New("harness: persistence error")
	ErrBroadcast     = errors.New("harness: broadcast error")
)
