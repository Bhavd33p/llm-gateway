package router

import (
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestBreaker(threshold int, cooldown time.Duration) (*Breaker, *fakeClock, *[]State) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	var transitions []State
	b := NewBreaker(threshold, cooldown, func(s State) { transitions = append(transitions, s) })
	b.now = clock.now
	return b, clock, &transitions
}

func TestBreakerOpensAfterConsecutiveFailures(t *testing.T) {
	b, _, _ := newTestBreaker(3, time.Second)
	b.Failure()
	b.Failure()
	b.Success() // a success resets the streak
	b.Failure()
	b.Failure()
	if b.State() != Closed || !b.Allow() {
		t.Fatalf("breaker should still be closed after a broken streak, state=%v", b.State())
	}
	b.Failure()
	if b.State() != Open || b.Allow() {
		t.Fatalf("breaker should be open after 3 consecutive failures, state=%v", b.State())
	}
}

func TestBreakerHalfOpenAllowsOneProbe(t *testing.T) {
	b, clock, transitions := newTestBreaker(1, 10*time.Second)
	b.Failure()
	clock.advance(9 * time.Second)
	if b.Allow() {
		t.Fatal("should reject during cooldown")
	}
	clock.advance(time.Second)
	if !b.Allow() {
		t.Fatal("should allow a probe once the cooldown elapses")
	}
	if b.State() != HalfOpen || b.Allow() {
		t.Fatal("only one probe may be in flight while half-open")
	}
	b.Success()
	if b.State() != Closed || !b.Allow() {
		t.Fatalf("successful probe should close the breaker, state=%v", b.State())
	}
	want := []State{Open, HalfOpen, Closed}
	if len(*transitions) != len(want) {
		t.Fatalf("transitions = %v, want %v", *transitions, want)
	}
	for i := range want {
		if (*transitions)[i] != want[i] {
			t.Fatalf("transitions = %v, want %v", *transitions, want)
		}
	}
}

func TestBreakerFailedProbeReopens(t *testing.T) {
	b, clock, _ := newTestBreaker(1, 10*time.Second)
	b.Failure()
	clock.advance(10 * time.Second)
	if !b.Allow() {
		t.Fatal("probe should be allowed")
	}
	b.Failure()
	if b.State() != Open || b.Allow() {
		t.Fatal("failed probe should reopen the breaker for a fresh cooldown")
	}
	clock.advance(10 * time.Second)
	if !b.Allow() {
		t.Fatal("next probe should be allowed after another cooldown")
	}
}

func TestBreakerAbandonedProbeExpires(t *testing.T) {
	b, clock, _ := newTestBreaker(1, 10*time.Second)
	b.Failure()
	clock.advance(10 * time.Second)
	b.Allow() // probe starts, outcome never reported
	clock.advance(5 * time.Second)
	if b.Allow() {
		t.Fatal("second probe should wait while the first may still be running")
	}
	clock.advance(5 * time.Second)
	if !b.Allow() {
		t.Fatal("an abandoned probe must not block the breaker forever")
	}
}
