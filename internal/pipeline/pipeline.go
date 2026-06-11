package pipeline

import (
	"context"

	"github.com/smarty/harness/v2/internal/contracts"
)

type Configuration struct {
	Monitor                contracts.Monitor
	Recoverer              contracts.Recoverer
	Serializer             contracts.Serializer
	Writer                 contracts.Writer
	Dispatcher             contracts.Dispatcher
	Types                  []any
	BurstCapacity          int
	PipelineBufferCapacity int
	ExecutionUnitSize      int
	SerializerCount        int
	ShedThreshold          float64
}

func Build(ctx context.Context, config Configuration) (result contracts.Pipeline) {
	var (
		batches = make(chan *batch, config.BurstCapacity)
		work1   = make(chan *unitOfWork, config.PipelineBufferCapacity)
		work2   = make(chan *unitOfWork, config.PipelineBufferCapacity)
		work3   = make(chan *unitOfWork, config.PipelineBufferCapacity)
		work4b  = make(chan *unitOfWork, config.PipelineBufferCapacity)
		work4a  = make(chan *unitOfWork, 1)
		work5   = make(chan *unitOfWork, config.PipelineBufferCapacity)
	)

	var (
		recovery    = newRecovery(ctx, config.Recoverer, work4a, wait, config.Monitor)
		entrypoint  = newEntrypoint(config.Monitor, batches, config.ShedThreshold)
		executor    = newExecution(config.Monitor, config.ExecutionUnitSize, batches, work1, newRouter(config.Types...))
		serializers = newFanOut(serializationFactory(config.Monitor, config.Serializer), config.SerializerCount, config.PipelineBufferCapacity, work1, work2)
		persistence = newPersistence(ctx, config.Monitor, work2, work3, config.Writer, wait)
		completion  = newCompletion(work3, work4b)
		broadcast   = newBroadcast(ctx, config.Monitor, work4a, work4b, work5, config.Dispatcher, wait)
		terminal    = newTerminal(work5)
	)

	var listeners []contracts.Listener
	listeners = append(listeners,
		recovery,
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

func serializationFactory(monitor contracts.Monitor, enc contracts.Serializer) stationFactory {
	return func(in, out chan *unitOfWork) contracts.Listener {
		return newSerialization(monitor, enc, in, out)
	}
}
