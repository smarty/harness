package pipeline

import (
	"context"
	"reflect"
	"time"

	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/internal/generic"
)

type execution struct {
	monitor     contracts.Monitor
	now         func() time.Time
	maxUnitSize int
	newUnit     func() *unitOfWork
	newMessage  func() *contracts.Message
	typeNames   map[reflect.Type]string
	input       chan *batch
	output      chan *unitOfWork
	executor    executor
	decorator   contracts.Decorator
}

func newExecution(
	monitor contracts.Monitor,
	now func() time.Time,
	maxUnitSize int,
	newUnit func() *unitOfWork,
	newMessage func() *contracts.Message,
	typeNames map[reflect.Type]string,
	input chan *batch,
	output chan *unitOfWork,
	exec executor,
	decorator contracts.Decorator,
) *execution {
	return &execution{
		monitor:     monitor,
		now:         now,
		maxUnitSize: maxUnitSize,
		newUnit:     newUnit,
		newMessage:  newMessage,
		typeNames:   typeNames,
		input:       input,
		output:      output,
		executor:    exec,
		decorator:   decorator,
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
		start := len(unit.results)
		for _, instruction := range batch.instructions {
			this.executor.Execute(instruction, func(outgoing ...any) {
				for _, result := range outgoing {
					message := this.newMessage()
					message.ID = 0
					message.Value = result
					message.Type = this.typeNames[reflect.TypeOf(message.Value)]
					message.Content = generic.ReclaimBuffer(message.Content, initialContentBufferSize)
					message.ContentType = ""
					unit.results = append(unit.results, message)
				}
			})
		}
		this.applyDecorator(batch.ctx, unit.results[start:])
		if len(unit.completions) < this.maxUnitSize && len(this.input) > 0 {
			continue // more to do
		}
		this.output <- unit
		unit = nil
	}
}

// applyDecorator hands each of the batch's produced values to the Decorator and
// writes the returned replacement back onto its message. It runs per-batch over
// exactly the slice that batch produced, so each value is decorated with its own
// context. A single timestamp is captured once for the batch and passed to every
// message it produced, so they all carry a consistent time. Per the Decorator
// contract the replacement must keep the same concrete Go type, since each
// message's Type name was derived from the original value before decoration runs.
func (this *execution) applyDecorator(ctx context.Context, produced []*contracts.Message) {
	if len(produced) == 0 {
		return
	}
	now := this.now()
	for _, message := range produced {
		message.Value = this.decorator.Decorate(ctx, now, message.Value)
	}
}
