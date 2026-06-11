package pipeline

import "github.com/smarty/harness/v2/contracts"

type terminal struct {
	input      chan *unitOfWork
	putUnit    func(*unitOfWork)
	putMessage func(*contracts.Message)
}

func newTerminal(
	input chan *unitOfWork,
	putUnit func(*unitOfWork),
	putMessage func(*contracts.Message),
) *terminal {
	return &terminal{
		input:      input,
		putUnit:    putUnit,
		putMessage: putMessage,
	}
}

func (this *terminal) Listen() {
	for unit := range this.input {
		for _, message := range unit.results {
			this.putMessage(message)
		}
		this.putUnit(unit)
	}
}
