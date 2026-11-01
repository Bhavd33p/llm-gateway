package router

import (
	"context"
	"math/rand/v2"
	"time"
)

// Backoff is exponential backoff with full jitter: the n-th retry sleeps a uniformly random duration in
// [0, min(Max, Base*2^n)]. Randomising the whole interval (rather than adding a little jitter to a fixed delay)
// spreads retries from many clients that failed at the same moment, so they don't hit the upstream in lockstep.
type Backoff struct {
	Base time.Duration
	Max  time.Duration
}

// Delay returns the sleep before retry number retry, where retry 0 is the first retry.
func (b Backoff) Delay(retry int) time.Duration {
	ceiling := b.Max
	// Base<<retry overflows for large retry; comparing against Max>>retry avoids computing it.
	if retry >= 0 && retry < 63 && b.Base <= b.Max>>retry {
		ceiling = b.Base << retry
	}
	if ceiling <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(ceiling) + 1))
}

// sleep waits for d unless ctx ends first, in which case it returns the context's error.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
