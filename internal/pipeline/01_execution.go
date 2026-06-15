package pipeline

import (
	"reflect"

	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/internal/generic"
)

type execution struct {
	monitor     contracts.Monitor
	maxUnitSize int
	newUnit     func() *unitOfWork
	newMessage  func() *contracts.Message
	typeNames   map[reflect.Type]string
	input       chan *batch
	output      chan *unitOfWork
	executor    executor
}

func newExecution(
	monitor contracts.Monitor,
	maxUnitSize int,
	newUnit func() *unitOfWork,
	newMessage func() *contracts.Message,
	typeNames map[reflect.Type]string,
	input chan *batch,
	output chan *unitOfWork,
	exec executor,
) *execution {
	return &execution{
		monitor:     monitor,
		maxUnitSize: maxUnitSize,
		newUnit:     newUnit,
		newMessage:  newMessage,
		typeNames:   typeNames,
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
			unit.results = generic.Reclaim(unit.results, workingMessageCapacity)
			unit.completions = generic.Reclaim(unit.completions, this.maxUnitSize)
		}
		unit.completions = append(unit.completions, batch.complete)
		for _, instruction := range batch.instructions {
			this.executor.Execute(instruction, func(outgoing ...any) {
				for _, result := range outgoing {
					message := this.newMessage()
					message.ID = 0
					message.Type = this.typeNames[reflect.TypeOf(result)]
					message.Value = result
					message.Content = generic.ReclaimBuffer(message.Content, initialContentBufferSize)
					message.ContentType = ""
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
