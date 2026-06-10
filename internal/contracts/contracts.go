package contracts

import "context"

// Interfaces common to many of our external and internal modules
type (
	Listener interface {
		Listen()
	}
	Handler interface {
		Handle(context.Context, ...any)
	}
)

// Exported collaborator interfaces — callers supply real implementations via Options.*
type (
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
