package pipeline

import (
	"github.com/smarty/harness/v2/contracts"
)

// Internal interfaces — discovered reflectively from domain types
// supplied via Options.Types(...)
type (
	executor interface {
		Execute(message any, broadcast func(...any))
	}
	applicator interface {
		Apply(message any)
	}
)

// Unexported value types shared across pipeline stages.
type (
	batch struct {
		instructions []any
		complete     func()
	}
	unitOfWork struct {
		results     []*contracts.Message
		completions []func()
	}
)
