package pipeline

import (
	"fmt"

	"github.com/smarty/harness/v2/internal/contracts"
)

type serialization struct {
	monitor    contracts.Monitor
	serializer Serializer
	input      chan *unitOfWork
	output     chan *unitOfWork
}

func newSerialization(monitor contracts.Monitor, enc Serializer, input, output chan *unitOfWork) *serialization {
	return &serialization{
		monitor:    monitor,
		serializer: enc,
		input:      input,
		output:     output,
	}
}

func (this *serialization) Listen() {
	defer close(this.output)
	for unit := range this.input {
		for _, message := range unit.results {
			err := this.serializer.Serialize(message.Content, message.Value)
			if err != nil {
				failure := contracts.SerializationError{
					Value: message.Value,
					Error: fmt.Errorf("%w: %w", contracts.ErrSerialization, err),
				}
				this.monitor.Track(failure)
				panic(failure.Error) // The caller has failed to produce only values that will serialize successfully.
			}
			message.ContentType = this.serializer.ContentType()
		}
		this.output <- unit
	}
}
