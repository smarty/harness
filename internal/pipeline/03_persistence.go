package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/contracts/monitoring"
)

type persistence struct {
	ctx     context.Context
	monitor contracts.Monitor
	input   chan *unitOfWork
	output  chan *unitOfWork
	writer  contracts.Writer
	wait    func(context.Context, time.Duration) error
	buffer  []*contracts.Message
}

func newPersistence(ctx context.Context, monitor contracts.Monitor, input, output chan *unitOfWork, writer contracts.Writer, wait func(context.Context, time.Duration) error) *persistence {
	return &persistence{
		ctx:     ctx,
		monitor: monitor,
		input:   input,
		output:  output,
		writer:  writer,
		wait:    wait,
		buffer:  make([]*contracts.Message, 0, 1024),
	}
}

func (this *persistence) Listen() {
	defer close(this.output)
	for unit := range this.input {
		for _, message := range unit.results {
			this.buffer = append(this.buffer, message)
		}
		stored := this.store()
		this.buffer = this.buffer[:0]
		if !stored {
			continue // shutdown before durable write: do NOT forward (no ack); MQ redelivers
		}
		this.output <- unit
	}
}
func (this *persistence) store() (stored bool) {
	var failure monitoring.PersistenceError
	for attempt := 1; ; attempt++ {
		err := this.writer.Write(this.ctx, this.buffer...)
		if err == nil {
			return true
		}
		failure.Attempt = attempt
		failure.Error = fmt.Errorf("%w: %w", monitoring.ErrPersistence, err)
		this.monitor.Track(failure)
		// Retries forever (until the process restarts) unless the context is cancelled.
		// TODO: exponential back-off w/ jitter
		if this.wait(this.ctx, time.Second) != nil {
			this.monitor.Track(monitoring.PersistenceAbandoned{Attempts: attempt})
			return false
		}
	}
}
