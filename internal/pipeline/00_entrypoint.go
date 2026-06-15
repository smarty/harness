package pipeline

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/contracts/monitoring"
	"github.com/smarty/harness/v2/internal/generic"
)

type entrypoint struct {
	monitor       contracts.Monitor
	waiters       *generic.PoolT[*sync.WaitGroup]
	batches       *generic.PoolT[*batch]
	work          chan *batch
	lock          *sync.RWMutex
	closed        bool
	closeOnce     sync.Once
	done          chan struct{}
	shedThreshold float64
}

func newEntrypoint(monitor contracts.Monitor, work chan *batch, shedThreshold float64) *entrypoint {
	return &entrypoint{
		monitor:       monitor,
		waiters:       generic.NewPoolT(generic.NewT[sync.WaitGroup]),
		batches:       generic.NewPoolT(generic.NewT[batch]),
		work:          work,
		lock:          new(sync.RWMutex),
		done:          make(chan struct{}),
		shedThreshold: shedThreshold,
	}
}

func (this *entrypoint) prepare(messages ...any) (waiter *sync.WaitGroup, item *batch, failed *atomic.Bool) {
	waiter = this.waiters.Get()
	waiter.Add(1)
	item = this.batches.Get()
	item.instructions = messages
	failed = new(atomic.Bool)
	item.complete = func(stored bool) {
		if !stored {
			failed.Store(true)
			this.monitor.Track(batchAbandoned)
		} else {
			this.monitor.Track(batchComplete)
		}
		waiter.Done()
		this.batches.Put(item)
	}
	return waiter, item, failed
}

func (this *entrypoint) abandon(waiter *sync.WaitGroup, item *batch) {
	waiter.Done()
	this.batches.Put(item)
}

func (this *entrypoint) waiterDone(waiter *sync.WaitGroup) (done chan struct{}) {
	done = make(chan struct{})
	go func() { waiter.Wait(); close(done) }()
	return done
}

func (this *entrypoint) Handle(_ context.Context, messages ...any) {
	this.lock.RLock()
	if this.closed {
		this.lock.RUnlock()
		return
	}

	waiter, item, failed := this.prepare(messages...)
	select {
	case this.work <- item:
		// fast path: a slot is available, take it deterministically rather than
		// racing the done case below once shutdown has closed it.
	default:
		select {
		case this.work <- item:
			// normal full-channel backpressure: block until a slot frees...
		case <-this.done:
			// ...but become escapable at shutdown so Close cannot deadlock behind
			// us. The work was never stored; releasing normally would let a broker
			// acknowledge unstored work, so abandon and panic instead.
			this.lock.RUnlock()
			this.abandon(waiter, item)
			this.waiters.Put(waiter)
			this.monitor.Track(batchAbandoned)
			panic(monitoring.ErrBatchAbandoned)
		}
	}
	this.lock.RUnlock()
	this.monitor.Track(batchInFlight)

	waiter.Wait()
	this.waiters.Put(waiter)
	if failed.Load() {
		// The work was never durably stored and never can be (context cancelled).
		// Returning normally would let message brokers acknowledge unstored work,
		// so escalate: the panic kills the process and the broker redelivers.
		panic(monitoring.ErrBatchAbandoned)
	}
}

func (this *entrypoint) await(ctx context.Context, message any) {
	this.lock.RLock()
	if this.closed {
		this.lock.RUnlock()
		return
	}

	waiter, batch, failed := this.prepare(message)

	select {
	case this.work <- batch:
		this.lock.RUnlock()
		this.monitor.Track(batchInFlight)
	default:
		select {
		case this.work <- batch:
			this.lock.RUnlock()
			this.monitor.Track(batchInFlight)
		case <-ctx.Done():
			this.lock.RUnlock()
			this.abandon(waiter, batch)
			this.waiters.Put(waiter) // safe: abandon() called Done() (count 0); no detached waiter.
			this.monitor.Track(callerDeparted)
			return
		case <-this.done:
			// Shutdown while blocked on a wedged downstream. Unlike a caller
			// departure, the work was never stored; panic rather than acknowledge
			// unstored work, so an HTTP stack returns 5xx rather than a false success.
			this.lock.RUnlock()
			this.abandon(waiter, batch)
			this.waiters.Put(waiter) // safe: abandon() called Done() (count 0); no detached waiter.
			this.monitor.Track(batchAbandoned)
			panic(monitoring.ErrBatchAbandoned)
		}
	}

	select {
	case <-this.waiterDone(waiter):
		this.waiters.Put(waiter) // safe: detached Wait() returned before done was closed (count 0).
		if failed.Load() {
			// Same contract as Handle: never return normally for unstored work.
			// HTTP stacks recover this panic into a 5xx rather than a false success.
			panic(monitoring.ErrBatchAbandoned)
		}
	case <-ctx.Done():
		this.monitor.Track(callerDeparted)
		// Intentionally do NOT recycle the waiter here: complete() is still pending
		// and the detached waiterDone goroutine is still inside Wait(). Returning it
		// to the pool now would let a later prepare() Add(1) before this Wait()
		// returns -- documented sync.WaitGroup misuse. Get() already removed the
		// pool's reference, so declining to Put() leaves nothing pool-held; the
		// waiter is released to GC once complete() fires.
	}
}

func (this *entrypoint) admit() bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	if this.closed {
		return false
	}
	if float64(len(this.work))/float64(cap(this.work)) >= this.shedThreshold {
		this.monitor.Track(loadShed)
		return false
	}
	return true
}

// Listen blocks until Close is called so the entrypoint can be added as a dominoes listener.
// This guarantees Close is invoked during shutdown, which closes the work channel and lets the
// downstream pipeline stages drain naturally.
func (this *entrypoint) Listen() { <-this.done }

// Close signals blocked senders to abandon (closing done) before it closes the
// work channel, so Close can never deadlock behind a Handle/await stuck mid-send
// on a wedged downstream. It is idempotent.
func (this *entrypoint) Close() error {
	this.closeOnce.Do(func() {
		// Close done BEFORE taking the write lock: a sender blocked on a full work
		// channel holds RLock, so Lock is unreachable until that sender observes
		// done and releases RLock. Only then can we safely close the work channel.
		close(this.done)
		this.lock.Lock()
		this.closed = true
		close(this.work)
		this.lock.Unlock()
	})
	return nil
}

var (
	batchInFlight  monitoring.BatchInFlight
	batchComplete  monitoring.BatchComplete
	batchAbandoned monitoring.BatchAbandoned
	loadShed       monitoring.LoadShed
	callerDeparted monitoring.CallerDeparted
)
