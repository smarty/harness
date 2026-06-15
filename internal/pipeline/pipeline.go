package pipeline

import (
	"bytes"
	"context"

	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/internal/generic"
)

func Build(ctx context.Context, config Configuration) (result contracts.Pipeline, err error) {
	err = config.validate()
	if err != nil {
		return result, err
	}
	var (
		batches = make(chan *batch, config.BurstCapacity)
		work1   = make(chan *unitOfWork, config.PipelineBufferCapacity)
		work2   = make(chan *unitOfWork, config.PipelineBufferCapacity)
		work3   = make(chan *unitOfWork, config.PipelineBufferCapacity)
		work4a  = make(chan *unitOfWork, config.PipelineBufferCapacity)
		work4b  = make(chan *unitOfWork, config.PipelineBufferCapacity)
		work5   = make(chan *unitOfWork, config.PipelineBufferCapacity)
	)
	var (
		unitPool    = generic.NewPoolT(generic.NewT[unitOfWork])
		messagePool = generic.NewPoolT(func() *contracts.Message {
			return &contracts.Message{Content: bytes.NewBuffer(make([]byte, 0, initialContentBufferSize))}
		})
	)

	entry := newEntrypoint(config.Monitor, batches, config.ShedThreshold)
	adapter := newHTTPAdapter(entry)
	result = contracts.Pipeline{
		SheddingHTTPWrapper: adapter.HTTPHandler,
		SheddingEntrypoint:  adapter,
		BlockingEntrypoint:  entry,
		Listeners: []contracts.Listener{
			newRecovery(ctx, config.Recoverer, recoveryBatchSize, work4a, wait, config.Monitor),
			entry,
			newExecution(config.Monitor, config.ExecutionUnitSize, unitPool.Get, messagePool.Get,
				config.MessageTypes, batches, work1, newRouter(config.DomainTypes...)),
			newSerialization(config.Monitor, config.Serializer, work1, work2),
			newPersistence(ctx, config.Monitor, work2, work3, config.Writer, wait),
			newCompletion(work3, work4b),
			newBroadcast(ctx, config.Monitor, work4a, work4b, work5, config.Dispatcher, wait),
			newTerminal(work5, unitPool.Put, messagePool.Put),
		},
	}
	return result, nil
}

const (
	recoveryBatchSize        = 64
	initialContentBufferSize = 2048

	// workingMessageCapacity is the steady-state capacity retained for the
	// per-unit and persistence message buffers across reuse; a unit larger than
	// this (e.g. one command broadcasting a huge burst of events) has its
	// oversized backing array discarded on recycle rather than pinned for the
	// life of the process. See generic.Reclaim.
	workingMessageCapacity = 256
)
