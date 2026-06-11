package contracts

import (
	"bytes"
	"context"
	"io"
)

// Interfaces common to many of our external and internal modules
type (
	Listener interface {
		Listen()
	}
	Handler interface {
		Handle(context.Context, ...any)
	}
)

// Collaborator interfaces — callers supply real implementations via Options.*
type (
	Serializer interface {
		Serialize(out io.Writer, in any) error
		ContentType() string
	}
	Writer interface {
		Write(ctx context.Context, messages ...*Message) error
	}
	Dispatcher interface {
		Dispatch(ctx context.Context, messages ...*Message) error
	}
	Monitor interface {
		Track(observation any)
	}
)

// Message represents a record to be saved or loaded to/from the Messages database table.
type Message struct {
	// ID represents the unique ID of this message and its sequential place within a larger stream.
	ID uint64

	// Type is the name registered for the (Go) type of the Value.
	// (i.e. 'subscription:subscription-renewed-v2').
	Type string

	// Value contains the in-memory Go message structure.
	Value any

	// Content contains the serialized representation of the Go Value.
	Content *bytes.Buffer

	// ContentType identifies the serialization method employed to represent the Content
	// (i.e. 'application/json; charset=utf-8').
	ContentType string
}
