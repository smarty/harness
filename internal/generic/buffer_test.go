package generic

import (
	"bytes"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
)

func TestReclaimBufferFixture(t *testing.T) {
	gunit.Run(new(ReclaimBufferFixture), t)
}

type ReclaimBufferFixture struct {
	*gunit.Fixture
}

func (this *ReclaimBufferFixture) TestWithinCapacity_ResetsAndKeepsSameBuffer() {
	buffer := bytes.NewBuffer(make([]byte, 0, 8))
	buffer.WriteString("hi")

	result := ReclaimBuffer(buffer, 8)

	this.So(result == buffer, should.BeTrue) // same instance, reused
	this.So(result.Len(), should.Equal, 0)
	this.So(result.Cap(), should.Equal, 8)
}

func (this *ReclaimBufferFixture) TestBeyondCapacity_ReplacesWithRightSizedBuffer() {
	buffer := bytes.NewBuffer(make([]byte, 0, 100))

	result := ReclaimBuffer(buffer, 8)

	this.So(result == buffer, should.BeFalse) // oversized buffer dropped
	this.So(result.Len(), should.Equal, 0)
	this.So(result.Cap(), should.Equal, 8)
}
