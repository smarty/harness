package pipeline

import (
	"context"
	"fmt"

	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/contracts/monitoring"
)

type broadcast struct {
	ctx        context.Context
	monitor    contracts.Monitor
	startup    chan *unitOfWork
	input      chan *unitOfWork
	output     chan *unitOfWork
	buffer     []*contracts.Message
	dispatcher contracts.Dispatcher
	wait       contracts.Waiter
}

func newBroadcast(
	ctx context.Context,
	monitor contracts.Monitor,
	startup, input, output chan *unitOfWork,
	dispatcher contracts.Dispatcher,
	wait contracts.Waiter,
) *broadcast {
	return &broadcast{
		ctx:        ctx,
		monitor:    monitor,
		startup:    startup,
		input:      input,
		output:     output,
		buffer:     make([]*contracts.Message, 0, 1024),
		dispatcher: dispatcher,
		wait:       wait,
	}
}

func (this *broadcast) Listen() {
	defer close(this.output)
	// Recovered (stored-but-undispatched) messages are dispatched before any
	// live traffic, so this station idles while a failing Recoverer retries
	// and the pipeline backs up behind it — deliberate; see Recovery.recover.
	this.processFrom(this.startup)
	this.processFrom(this.input)
}

func (this *broadcast) processFrom(input chan *unitOfWork) {
	for unit := range input {
		for _, message := range unit.results {
			this.buffer = append(this.buffer, message)
		}
		this.dispatch()
		this.buffer = this.buffer[:0]
		// Unlike persistence (which drops the unit on abandonment so MQ redelivers),
		// broadcast always forwards: the batch is already durably stored, so we ack
		// upstream regardless of whether dispatch succeeded.
		this.output <- unit
	}
}
func (this *broadcast) dispatch() {
	for attempt := 1; ; attempt++ {
		err := this.dispatcher.Dispatch(this.ctx, this.buffer...)
		if err == nil {
			return
		}
		this.monitor.Track(monitoring.BroadcastError{
			Attempt: attempt,
			Error:   fmt.Errorf("%w: %w", monitoring.ErrBroadcast, err),
		})
		// Retries forever (until the process restarts) unless the context is cancelled.
		if this.wait(this.ctx, backoff(attempt)) != nil {
			this.monitor.Track(monitoring.BroadcastAbandoned{Attempts: attempt})
			return
		}
	}
}
