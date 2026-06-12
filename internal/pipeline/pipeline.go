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
			return &contracts.Message{Content: bytes.NewBuffer(make([]byte, initialContentBufferSize))}
		})
	)

	var (
		recovery    = newRecovery(ctx, config.Recoverer, recoveryBatchSize, work4a, wait, config.Monitor)
		entrypoint  = newEntrypoint(config.Monitor, batches, config.ShedThreshold)
		executor    = newExecution(config.Monitor, config.ExecutionUnitSize, unitPool.Get, messagePool.Get, config.MessageTypes, batches, work1, newRouter(config.DomainTypes...))
		serializers = newFanOut(serializationFactory(config.Monitor, config.Serializer), config.SerializerCount, config.PipelineBufferCapacity, work1, work2)
		persistence = newPersistence(ctx, config.Monitor, work2, work3, config.Writer, wait)
		completion  = newCompletion(work3, work4b)
		broadcast   = newBroadcast(ctx, config.Monitor, work4a, work4b, work5, config.Dispatcher, wait)
		terminal    = newTerminal(work5, unitPool.Put, messagePool.Put)
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
	result = contracts.Pipeline{
		SheddingHTTPWrapper: adapter.HTTPHandler,
		SheddingEntrypoint:  adapter,
		BlockingEntrypoint:  entrypoint,
		Listeners:           listeners,
	}
	return result, nil
}

func serializationFactory(monitor contracts.Monitor, enc contracts.Serializer) stationFactory {
	return func(in, out chan *unitOfWork) contracts.Listener {
		return newSerialization(monitor, enc, in, out)
	}
}

const (
	recoveryBatchSize        = 64
	initialContentBufferSize = 1024
)
