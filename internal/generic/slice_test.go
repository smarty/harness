package generic

import (
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
)

func TestReclaimFixture(t *testing.T) {
	gunit.Run(new(ReclaimFixture), t)
}

type ReclaimFixture struct {
	*gunit.Fixture
}

func (this *ReclaimFixture) TestWithinCapacity_TruncatesClearsAndKeepsBackingArray() {
	s := make([]int, 3, 8)
	s[0], s[1], s[2] = 1, 2, 3

	result := Reclaim(s, 8)

	this.So(len(result), should.Equal, 0)
	this.So(cap(result), should.Equal, 8)        // backing array reused, capacity preserved
	this.So(s[:3], should.Equal, []int{0, 0, 0}) // elements cleared so they can be collected
}

func (this *ReclaimFixture) TestAtCapacity_KeepsBackingArray() {
	result := Reclaim(make([]int, 0, 8), 8)

	this.So(cap(result), should.Equal, 8)
}

func (this *ReclaimFixture) TestBeyondCapacity_DiscardsOversizedArrayAndRestoresWorkingCapacity() {
	result := Reclaim(make([]int, 0, 100), 8)

	this.So(len(result), should.Equal, 0)
	this.So(cap(result), should.Equal, 8) // oversized array dropped; restored to working capacity in one allocation
}
