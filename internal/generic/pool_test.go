package generic

import (
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
)

func TestPoolFixture(t *testing.T) {
	gunit.Run(new(PoolFixture), t)
}

type PoolFixture struct {
	*gunit.Fixture
}

func (this *PoolFixture) TestNewT_ReturnsPointerToZeroValue() {
	value := NewT[int]()

	this.So(value, should.NOT.BeNil)
	this.So(*value, should.Equal, 0)
}

func (this *PoolFixture) TestEmptyPool_GetInvokesNewFunc() {
	calls := 0
	pool := NewPoolT(func() *int { calls++; return new(int) })

	value := pool.Get()

	this.So(value, should.NOT.BeNil)
	this.So(calls, should.Equal, 1)
}

func (this *PoolFixture) TestPutThenGet_RecyclesValueWithoutInvokingNewFunc() {
	calls := 0
	pool := NewPoolT(func() *int { calls++; return new(int) })
	recycled := new(int)

	pool.Put(recycled)
	value := pool.Get()

	if raceDetectorEnabled && calls > 0 {
		return // sync.Pool dropped the Put on the floor (see pool_race_test.go); recycling is unobservable on this run
	}
	this.So(value, should.Equal, recycled)
	this.So(calls, should.Equal, 0)
}
