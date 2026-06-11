package generic

import (
	"slices"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
)

func TestDrainFixture(t *testing.T) {
	gunit.Run(new(DrainFixture), t)
}

type DrainFixture struct {
	*gunit.Fixture
	input chan int
}

func (this *DrainFixture) Setup() {
	this.input = make(chan int, 4)
}

func (this *DrainFixture) TestEmptyClosedChannel_YieldsNothing() {
	close(this.input)

	results := slices.Collect(Drain(this.input))

	this.So(results, should.BeEmpty)
}

func (this *DrainFixture) TestYieldsAllValuesInOrderUntilChannelCloses() {
	this.input <- 1
	this.input <- 2
	this.input <- 3
	close(this.input)

	results := slices.Collect(Drain(this.input))

	this.So(results, should.Equal, []int{1, 2, 3})
}

func (this *DrainFixture) TestEarlyBreak_StopsYieldingAndLeavesRemainderInChannel() {
	this.input <- 1
	this.input <- 2
	this.input <- 3
	close(this.input)

	var firstOnly []int
	for v := range Drain(this.input) {
		firstOnly = append(firstOnly, v)
		break
	}
	remaining := slices.Collect(Drain(this.input))

	this.So(firstOnly, should.Equal, []int{1})
	this.So(remaining, should.Equal, []int{2, 3})
}
