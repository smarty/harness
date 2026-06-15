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

// Listen pages the backlog: it pulls one page at a time (retrying each with
// backoff) and forwards it downstream until the Recoverer returns an empty
// page, then tracks a single RecoveryComplete carrying the total recovered.
// Channel backpressure bounds resident memory to a handful of pages no matter
// how large the backlog is.
//
// While a page retries, broadcast waits on this station's output before serving
// live traffic, so a persistently failing Recoverer backs up the entire
// pipeline: completion, then persistence, then entrypoint callers. That is
// deliberate: the Recoverer reads from the same datastore the Writer writes
// to, so if undispatched messages cannot be recovered, new messages cannot
// be durably written either, and there is no live work worth admitting.
// Operators see RecoveryError observations while dispatching is stopped;
// the cure is restoring the datastore (or shutdown, which abandons here
// just like persistence/broadcast and lets the next start retry recovery).
func (this *Recovery) Listen() {
	defer close(this.output)

	total := 0
	for {
		page, ok := this.recoverPage()
		if !ok {
			return // shutdown abandoned the retry loop; no RecoveryComplete
		}
		if len(page) == 0 {
			this.monitor.Track(monitoring.RecoveryComplete{Count: total})
			return
		}
		total += len(page)
		this.forward(page)
	}
}

// recoverPage retries the next page until the Recoverer succeeds or the context
// is cancelled. On success it returns (page, true); the page may be empty, which
// signals recovery is complete. On shutdown it tracks RecoveryAbandoned and
// returns (nil, false). The failure-streak counter starts fresh each call, so
// backoff measures consecutive failures, not lifetime calls.
func (this *Recovery) recoverPage() ([]*contracts.Message, bool) {
	for attempt := 1; ; attempt++ {
		page, err := this.recoverer.Recover(this.ctx, this.batchSize)
		if err == nil {
			return page, true
		}
		this.monitor.Track(monitoring.RecoveryError{
			Attempt: attempt,
			Error:   fmt.Errorf("%w: %w", monitoring.ErrRecovery, err),
		})
		// Retries forever (until the process restarts) unless the context is cancelled.
		if this.wait(this.ctx, backoff(attempt)) != nil {
			this.monitor.Track(monitoring.RecoveryAbandoned{Attempts: attempt})
			return nil, false
		}
	}
}

// forward splits a page into batchSize-bounded units, defending against a
// Recoverer that returns more than the limit it was given.
func (this *Recovery) forward(messages []*contracts.Message) {
	for len(messages) > this.batchSize {
		this.output <- &unitOfWork{results: messages[:this.batchSize]}
		messages = messages[this.batchSize:]
	}
	this.output <- &unitOfWork{results: messages}
}
