package pipeline

import (
	"io"

	"github.com/smarty/harness/v2/internal/contracts"
)

type Configuration struct {
	Monitor                contracts.Monitor
	Serializer             Serializer
	Writer                 contracts.Writer
	Dispatcher             contracts.Dispatcher
	Types                  []any
	BurstCapacity          int
	PipelineBufferCapacity int
	ExecutionUnitSize      int
	SerializerCount        int
	ShedThreshold          float64
}

// Internal interfaces — discovered reflectively from domain types
// supplied via Options.Types(...)
type (
	Executor interface {
		Execute(message any, broadcast func(...any))
	}
	Applicator interface {
		Apply(message any)
	}
	Serializer interface {
		Serialize(out io.Writer, in any) error
		ContentType() string
	}
)

// Unexported value types shared across pipeline stages.
type (
	batch struct {
		messages []any
		complete func()
	}
	unitOfWork struct {
		results     []*contracts.Message
		completions []func()
	}
)
