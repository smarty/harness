package pipeline

import (
	"sync"

	"github.com/smarty/harness/v2/contracts"
)

type stationFactory func(in, out chan *unitOfWork) contracts.Listener

func newFanOut(factory stationFactory, workerCount, unitCapacity int, input, finalOutput chan *unitOfWork) []contracts.Listener {
	var (
		listeners = make([]contracts.Listener, workerCount)
		outputs   = make([]chan *unitOfWork, workerCount)
	)
	for i := range workerCount {
		outputs[i] = make(chan *unitOfWork, unitCapacity)
		listeners[i] = factory(input, outputs[i])
	}
	return append(listeners, newFanIn(outputs, finalOutput))
}

type fanIn struct {
	inputs []chan *unitOfWork
	output chan *unitOfWork
}

func newFanIn(inputs []chan *unitOfWork, output chan *unitOfWork) *fanIn {
	return &fanIn{
		inputs: inputs,
		output: output,
	}
}

func (this *fanIn) Listen() {
	defer close(this.output)
	var waiter sync.WaitGroup
	for _, input := range this.inputs {
		waiter.Go(func() {
			for unit := range input {
				this.output <- unit
			}
		})
	}
	waiter.Wait()
}
