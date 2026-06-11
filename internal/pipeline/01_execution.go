package pipeline

import (
	"github.com/smarty/harness/v2/contracts"
)

type execution struct {
	monitor     contracts.Monitor
	maxUnitSize int
	newUnit     func() *unitOfWork
	newMessage  func() *contracts.Message
	input       chan *batch
	output      chan *unitOfWork
	executor    executor
}

func newExecution(
	monitor contracts.Monitor,
	maxUnitSize int,
	newUnit func() *unitOfWork,
	newMessage func() *contracts.Message,
	input chan *batch,
	output chan *unitOfWork,
	exec executor,
) *execution {
	return &execution{
		monitor:     monitor,
		maxUnitSize: maxUnitSize,
		newUnit:     newUnit,
		newMessage:  newMessage,
		input:       input,
		output:      output,
		executor:    exec,
	}
}

func (this *execution) Listen() {
	defer close(this.output)

	var unit *unitOfWork
	for batch := range this.input {
		if unit == nil {
			unit = this.newUnit()
			clear(unit.results)
			clear(unit.completions)
			unit.results = unit.results[:0]
			unit.completions = unit.completions[:0]
		}
		unit.completions = append(unit.completions, batch.complete)
		for _, instruction := range batch.instructions {
			this.executor.Execute(instruction, func(outgoing ...any) {
				for _, result := range outgoing {
					message := this.newMessage()
					message.Value = result
					message.Content.Reset()
					unit.results = append(unit.results, message)
				}
			})
		}
		if len(unit.completions) < this.maxUnitSize && len(this.input) > 0 {
			continue // more to do
		}
		this.output <- unit
		unit = nil
	}
}
