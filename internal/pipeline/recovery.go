package pipeline

import (
	"context"

	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/contracts/monitoring"
)

type Recovery struct {
	ctx       context.Context
	recoverer contracts.Recoverer
	batchSize int
	output    chan *unitOfWork
	wait      contracts.Waiter
	monitor   contracts.Monitor
}

func newRecovery(
	ctx context.Context,
	recoverer contracts.Recoverer,
	batchSize int,
	output chan *unitOfWork,
	wait contracts.Waiter,
	monitor contracts.Monitor,
) *Recovery {
	return &Recovery{
		ctx:       ctx,
		recoverer: recoverer,
		batchSize: batchSize,
		output:    output,
		wait:      wait,
		monitor:   monitor,
	}
}

func (this *Recovery) Listen() {
	defer close(this.output)

	if messages := this.recover(); len(messages) > 0 {
		for len(messages) > this.batchSize {
			this.output <- &unitOfWork{results: messages[:this.batchSize]}
			messages = messages[this.batchSize:]
		}
		this.output <- &unitOfWork{results: messages}
	}
}

func (this *Recovery) recover() []*contracts.Message {
	for attempt := 1; ; attempt++ {
		results, err := this.recoverer.Recover(this.ctx)
		if err == nil {
			return results
		}
		this.monitor.Track(monitoring.RecoveryError{Attempt: attempt, Error: err})

		if this.wait(this.ctx, backoff(attempt)) != nil {
			return nil
		}
	}
}
