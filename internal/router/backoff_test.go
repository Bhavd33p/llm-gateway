package router

import (
	"context"
	"testing"
	"time"
)

func TestBackoffStaysWithinExponentialCeiling(t *testing.T) {
	b := Backoff{Base: 10 * time.Millisecond, Max: 500 * time.Millisecond}
	ceilings := []time.Duration{10, 20, 40, 80, 160, 320, 500, 500, 500}
	for retry, ceiling := range ceilings {
		ceiling *= time.Millisecond
		var maxSeen time.Duration
		for range 2000 {
			d := b.Delay(retry)
			if d < 0 || d > ceiling {
				t.Fatalf("retry %d: delay %v outside [0, %v]", retry, d, ceiling)
			}
			maxSeen = max(maxSeen, d)
		}
		// Full jitter spreads over the whole interval, so the largest of 2000 samples should be near the ceiling.
		if maxSeen < ceiling*9/10 {
			t.Errorf("retry %d: max delay %v never approached ceiling %v", retry, maxSeen, ceiling)
		}
	}
}

func TestBackoffDoesNotOverflow(t *testing.T) {
	b := Backoff{Base: time.Second, Max: 30 * time.Second}
	for _, retry := range []int{40, 62, 63, 100, 1 << 20} {
		if d := b.Delay(retry); d < 0 || d > b.Max {
			t.Fatalf("retry %d: delay %v outside [0, %v]", retry, d, b.Max)
		}
	}
}

func TestSleepStopsWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if err := sleep(ctx, time.Hour); err == nil {
		t.Fatal("sleep should return the context error")
	}
	if time.Since(start) > time.Second {
		t.Fatal("sleep ignored cancellation")
	}
}
