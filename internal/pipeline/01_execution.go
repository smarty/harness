package pipeline

import (
	"context"
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
	decorator   contracts.Decorator
	values      []any // reusable per-batch scratch buffer for the decorator.
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
	decorator contracts.Decorator,
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
					message.Type = this.typeNames[reflect.TypeOf(result)]
					message.Value = result
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

// applyDecorator hands the batch's produced values to the Decorator and writes
// any replacements back onto their messages. It runs per-batch over exactly the
// slice that batch produced, so each value is decorated with its own context. A
// decorator that returns a slice of a different length is ignored (the messages
// are left intact) — write-back only happens when the contract is honored.
func (this *execution) applyDecorator(ctx context.Context, produced []*contracts.Message) {
	if len(produced) == 0 {
		return
	}
	this.values = this.values[:0]
	for _, message := range produced {
		this.values = append(this.values, message.Value)
	}
	this.values = this.decorator.Decorate(ctx, this.values)
	if len(this.values) != len(produced) {
		return
	}
	for i := range produced {
		produced[i].Value = this.values[i]
	}
}
