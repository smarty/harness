package snapshots

import "errors"

// logger receives informational progress messages during initialization.
// A *log.Logger satisfies it.
type logger interface {
	Printf(format string, args ...any)
}

// applicator is the domain being rebuilt: InitializeDomain calls Apply once with
// the decoded snapshot, then once per replayed event in ascending id order.
type applicator interface {
	Apply(message any)
}

var (
	errMissingSnapshot        = errors.New("snapshots: no snapshot found")
	errUnsupportedMessageType = errors.New("snapshots: unsupported message type")
)
