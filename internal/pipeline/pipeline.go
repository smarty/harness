package pipeline

import (
	"context"

	"github.com/smarty/harness/v2/internal/contracts"
)

func Build(ctx context.Context, config Configuration) (result contracts.Pipeline) {
	var (
		batches = make(chan *batch, config.BurstCapacity)
		work1   = make(chan *unitOfWork, config.PipelineBufferCapacity)
		work2   = make(chan *unitOfWork, config.PipelineBufferCapacity)
		work3   = make(chan *unitOfWork, config.PipelineBufferCapacity)
		work4   = make(chan *unitOfWork, config.PipelineBufferCapacity)
		work5   = make(chan *unitOfWork, config.PipelineBufferCapacity)
	)

	var (
		entrypoint  = newEntrypoint(config.Monitor, batches, config.ShedThreshold)
		executor    = newExecution(config.Monitor, config.ExecutionUnitSize, batches, work1, newRouter(config.Types...))
		serializers = newFanOut(serializationFactory(config.Monitor, config.Serializer), config.SerializerCount, config.PipelineBufferCapacity, work1, work2)
		persistence = newPersistence(ctx, config.Monitor, work2, work3, config.Writer, wait)
		completion  = newCompletion(work3, work4)
		broadcast   = newBroadcast(ctx, config.Monitor, work4, work5, config.Dispatcher, wait)
		terminal    = newTerminal(work5)
	)

	var listeners []contracts.Listener
	listeners = append(listeners,
		entrypoint,
		executor,
	)
	listeners = append(listeners, serializers...)
	listeners = append(listeners,
		persistence,
		completion,
		broadcast,
		terminal,
	)
	adapter := newHTTPAdapter(entrypoint)
	return contracts.Pipeline{
		SheddingHTTPWrapper: adapter.HTTPHandler,
		SheddingEntrypoint:  adapter,
		BlockingEntrypoint:  entrypoint,
		Listeners:           listeners,
	}
}

func serializationFactory(monitor contracts.Monitor, enc Serializer) stationFactory {
	return func(in, out chan *unitOfWork) contracts.Listener {
		return newSerialization(monitor, enc, in, out)
	}
}
