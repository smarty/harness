package pipeline

import (
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
)

func TestCompletionFixture(t *testing.T) {
	gunit.Run(new(CompletionFixture), t)
}

type CompletionFixture struct {
	*gunit.Fixture
	input   chan *unitOfWork
	output  chan *unitOfWork
	subject *completion
}

func (this *CompletionFixture) Setup() {
	this.input = make(chan *unitOfWork, 4)
	this.output = make(chan *unitOfWork, 4)
	this.subject = newCompletion(this.input, this.output)
}

func (this *CompletionFixture) drain() (results []*unitOfWork) {
	for unit := range this.output {
		results = append(results, unit)
	}
	return results
}

func (this *CompletionFixture) TestCallsAllCompletionsThenForwards() {
	var invocations []string
	var outcomes []bool
	this.input <- &unitOfWork{completions: []func(stored bool){
		func(stored bool) { invocations = append(invocations, "first"); outcomes = append(outcomes, stored) },
		func(stored bool) { invocations = append(invocations, "second"); outcomes = append(outcomes, stored) },
	}}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 1)
	this.So(invocations, should.Equal, []string{"first", "second"})
	this.So(outcomes, should.Equal, []bool{true, true})
}

func (this *CompletionFixture) TestNoCompletionsForwardsCleanly() {
	this.input <- &unitOfWork{}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 1)
}

func (this *CompletionFixture) TestEachUnitFiresIndependently() {
	var firstCalled, secondCalled int
	this.input <- &unitOfWork{completions: []func(stored bool){func(bool) { firstCalled++ }}}
	this.input <- &unitOfWork{completions: []func(stored bool){func(bool) { secondCalled++ }}}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 2)
	this.So(firstCalled, should.Equal, 1)
	this.So(secondCalled, should.Equal, 1)
}

func (this *CompletionFixture) TestClosedInputClosesOutput() {
	close(this.input)
	go this.subject.Listen()

	_, open := <-this.output
	this.So(open, should.BeFalse)
}
