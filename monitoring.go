package harness

import "errors"

// Monitor observations emitted by the pipeline.
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
)

var (
	ErrSerialization = errors.New("serialization error")
	ErrPersistence   = errors.New("persistence error")
	ErrBroadcast     = errors.New("broadcast error")
)
