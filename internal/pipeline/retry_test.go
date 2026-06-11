package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert"
	"github.com/smarty/gunit/v2/assert/should"
)

func TestWaitFixture(t *testing.T) {
	gunit.Run(new(WaitFixture), t)
}

type WaitFixture struct {
	*gunit.Fixture
}

func (this *WaitFixture) TestContextAlreadyCancelled_ReturnImmediately() {
	ctx, cancel := context.WithCancel(this.Context())
	cancel()

	started := time.Now()
	err := wait(ctx, time.Hour)
	ended := time.Since(started)

	this.So(err, should.Equal, context.Canceled)
	this.So(ended, should.BeLessThan, time.Second)
}

func (this *WaitFixture) TestWait() {
	const duration = time.Millisecond * 5

	started := time.Now()
	err := wait(this.Context(), duration)
	ended := time.Since(started)

	this.So(err, should.BeNil)
	this.So(ended, should.BeGreaterThanOrEqualTo, duration)
}

// assertBackoffWaits verifies each recorded wait falls in the jitter window
// for its attempt: [w/2, w) where w = min(1s<<n, 30s).
func assertBackoffWaits(t gunit.TestingT, waits []time.Duration, count int) {
	t.Helper()
	assert.So(t, waits, should.HaveLength, count)
	for n, d := range waits {
		window := min(time.Second<<n, 30*time.Second)
		assert.So(t, d, should.BeGreaterThanOrEqualTo, window/2)
		assert.So(t, d, should.BeLessThan, window)
	}
}

func TestBackoffFixture(t *testing.T) {
	gunit.Run(new(BackoffFixture), t)
}

type BackoffFixture struct {
	*gunit.Fixture
}

func (this *BackoffFixture) assertWindow(attempt int, uncapped time.Duration) {
	for range 100 {
		d := backoff(attempt)
		this.So(d, should.BeGreaterThanOrEqualTo, uncapped/2)
		this.So(d, should.BeLessThan, uncapped)
	}
}

func (this *BackoffFixture) TestExponentialWindows() {
	this.assertWindow(1, time.Second)
	this.assertWindow(2, 2*time.Second)
	this.assertWindow(3, 4*time.Second)
	this.assertWindow(4, 8*time.Second)
	this.assertWindow(5, 16*time.Second)
}

func (this *BackoffFixture) TestCapAtThirtySeconds() {
	this.assertWindow(6, 30*time.Second)
	this.assertWindow(7, 30*time.Second)
	this.assertWindow(100, 30*time.Second)
	this.assertWindow(1<<30, 30*time.Second)
}

func (this *BackoffFixture) TestNonPositiveAttemptTreatedAsFirst() {
	this.assertWindow(0, time.Second)
	this.assertWindow(-1, time.Second)
}

func (this *BackoffFixture) TestJitterVaries() {
	seen := make(map[time.Duration]bool)
	for range 100 {
		seen[backoff(6)] = true
	}
	this.So(len(seen), should.BeGreaterThan, 1)
}
