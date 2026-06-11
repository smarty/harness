package pipeline

import (
	"context"
	"math/rand/v2"
	"time"
)

// wait sleeps for d, or returns early with ctx.Err() if ctx is cancelled first.
func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// backoff generates a duration based on the attempt number.
// Generally, as the attempt increasing the duration increases by a factor of 2,
// but with jitter from the duration/2 up to the duration. The max duration is 30s.
func backoff(attempt int) time.Duration {
	result := time.Second
	for range min(attempt, 6) - 1 {
		result *= 2
	}
	result = min(result, time.Second*30)
	scope := result / 2
	return rand.N[time.Duration](scope) + scope
}
