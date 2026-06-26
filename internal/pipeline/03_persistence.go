package pipeline

import (
	"context"
	"fmt"

	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/contracts/monitoring"
	"github.com/smarty/harness/v2/internal/generic"
)

type persistence struct {
	ctx     context.Context
	monitor contracts.Monitor
	input   chan *unitOfWork
	output  chan *unitOfWork
	writer  writer
	wait    waiter
	buffer  []*contracts.Message
}

func newPersistence(ctx context.Context, monitor contracts.Monitor, input, output chan *unitOfWork, writer writer, wait waiter) *persistence {
	return &persistence{
		ctx:     ctx,
		monitor: monitor,
		input:   input,
		output:  output,
		writer:  writer,
		wait:    wait,
		buffer:  make([]*contracts.Message, 0, workingMessageCapacity),
	}
}

func (this *persistence) Listen() {
	defer close(this.output)
	for unit := range this.input {
		if len(unit.results) == 0 {
			this.output <- unit // nothing to persist; forward so completions fire.
			continue
		}
		for _, message := range unit.results {
			this.buffer = append(this.buffer, message)
		}
		stored := this.store()
		this.buffer = generic.Reclaim(this.buffer, workingMessageCapacity)
		if !stored {
			// Shutdown before durable write: do NOT forward (no ack); MQ redelivers.
			// Failing the completions lets blocked entrypoint callers escape (by panicking).
			for _, complete := range unit.completions {
				complete(false)
			}
			continue
		}
		this.monitor.Track(monitoring.ResultsPersisted{Count: len(unit.results)})
		this.output <- unit
	}
}
func (this *persistence) store() (stored bool) {
	for attempt := 1; ; attempt++ {
		err := this.writer.Write(this.ctx, this.buffer...)
		if err == nil {
			return true
		}
		this.monitor.Track(monitoring.PersistenceError{
			Attempt: attempt,
			Error:   fmt.Errorf("%w: %w", monitoring.ErrPersistence, err),
		})
		// Retries forever (until the process restarts) unless the context is cancelled.
		if this.wait(this.ctx, backoff(attempt)) != nil {
			this.monitor.Track(monitoring.PersistenceAbandoned{Attempts: attempt})
			return false
		}
	}
}
