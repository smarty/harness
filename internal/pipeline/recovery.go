package pipeline

import (
	"context"
	"fmt"

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

// recover retries until the Recoverer succeeds or the context is cancelled.
// While it retries, broadcast waits on this station's output before serving
// live traffic, so a persistently failing Recoverer backs up the entire
// pipeline: completion, then persistence, then entrypoint callers. That is
// deliberate: the Recoverer reads from the same datastore the Writer writes
// to, so if undispatched messages cannot be recovered, new messages cannot
// be durably written either, and there is no live work worth admitting.
// Operators see RecoveryError observations while dispatching is stopped;
// the cure is restoring the datastore (or shutdown, which abandons here
// just like persistence/broadcast and lets the next start retry recovery).
func (this *Recovery) recover() []*contracts.Message {
	for attempt := 1; ; attempt++ {
		results, err := this.recoverer.Recover(this.ctx)
		if err == nil {
			this.monitor.Track(monitoring.RecoveryComplete{Count: len(results)})
			return results
		}
		this.monitor.Track(monitoring.RecoveryError{
			Attempt: attempt,
			Error:   fmt.Errorf("%w: %w", monitoring.ErrRecovery, err),
		})
		// Retries forever (until the process restarts) unless the context is cancelled.
		if this.wait(this.ctx, backoff(attempt)) != nil {
			this.monitor.Track(monitoring.RecoveryAbandoned{Attempts: attempt})
			return nil
		}
	}
}
