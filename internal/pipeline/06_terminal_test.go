package pipeline

import (
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/internal/generic"
)

func TestTerminalFixture(t *testing.T) {
	gunit.Run(new(TerminalFixture), t)
}

type TerminalFixture struct {
	*gunit.Fixture
	input    chan *unitOfWork
	units    *CountingPoolT[*unitOfWork]
	messages *CountingPoolT[*contracts.Message]
	subject  *terminal
}

func (this *TerminalFixture) Setup() {
	this.input = make(chan *unitOfWork, 4)
	this.units = NewCountingPoolT(generic.NewT[unitOfWork])
	this.messages = NewCountingPoolT(generic.NewT[contracts.Message])
	this.subject = newTerminal(this.input, this.units.Put, this.messages.Put)
}

func (this *TerminalFixture) TestDrainsInputUntilClosed() {
	a := this.units.Get()
	b := this.units.Get()
	c := this.units.Get()

	a.results = append(a.results, this.messages.Get())                                           // 1
	b.results = append(b.results, this.messages.Get(), this.messages.Get())                      // 2
	c.results = append(c.results, this.messages.Get(), this.messages.Get(), this.messages.Get()) // 3 (sum: 6)

	this.input <- a
	this.input <- b
	this.input <- c

	close(this.input)

	done := make(chan struct{})
	go func() {
		this.subject.Listen()
		close(done)
	}()

	<-done
	this.So(len(this.input), should.Equal, 0)
	this.So(this.units.puts, should.Equal, 3)
	this.So(this.messages.puts, should.Equal, 6)
}

func (this *TerminalFixture) TestEmptyClosedInputReturnsImmediately() {
	close(this.input)

	done := make(chan struct{})
	go func() {
		this.subject.Listen()
		close(done)
	}()

	<-done
}

type CountingPoolT[T any] struct {
	pool *generic.PoolT[T]
	gets int
	puts int
}

func NewCountingPoolT[T any](new func() T) *CountingPoolT[T] {
	return &CountingPoolT[T]{
		pool: generic.NewPoolT(new),
	}
}

func (this *CountingPoolT[T]) Get() T {
	this.gets++
	return this.pool.Get()
}
func (this *CountingPoolT[T]) Put(t T) {
	this.puts++
	this.pool.Put(t)
}
