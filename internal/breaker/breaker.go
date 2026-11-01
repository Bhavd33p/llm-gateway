// Package breaker is a consecutive-failure circuit breaker, shared by the router (one per upstream) and the Redis
// client (one for the whole connection).
package breaker

import (
	"sync"
	"time"
)

type State int

const (
	Closed   State = iota // requests flow normally
	HalfOpen              // cooldown elapsed: one probe request decides whether to close or reopen
	Open                  // failing: requests skip this upstream until the cooldown elapses
)

func (s State) String() string {
	return [...]string{"closed", "half-open", "open"}[s]
}

// Breaker opens after a run of consecutive failures, so callers stop spending time on a dependency that is clearly
// down, and lets a single probe test it after a cooldown.
type Breaker struct {
	mu        sync.Mutex
	threshold int
	cooldown  time.Duration
	now       func() time.Time
	onChange  func(State)

	state        State
	failures     int
	openedAt     time.Time
	probeStarted time.Time
	probing      bool
}

func New(threshold int, cooldown time.Duration, onChange func(State)) *Breaker {
	if onChange == nil {
		onChange = func(State) {}
	}
	return &Breaker{threshold: threshold, cooldown: cooldown, now: time.Now, onChange: onChange}
}

// Allow reports whether a request may be sent now. While half-open it admits one probe at a time; a probe whose
// outcome is never reported (say, its client hung up) stops blocking others after one more cooldown.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	switch b.state {
	case Open:
		if now.Sub(b.openedAt) < b.cooldown {
			return false
		}
		b.setState(HalfOpen)
		fallthrough
	case HalfOpen:
		if b.probing && now.Sub(b.probeStarted) < b.cooldown {
			return false
		}
		b.probing, b.probeStarted = true, now
		return true
	default:
		return true
	}
}

// Success closes a half-open breaker (the probe worked) and resets the failure streak of a closed one. While open
// it is ignored: a slow request that started before the breaker tripped must not skip the cooldown and probe.
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case HalfOpen:
		b.failures, b.probing = 0, false
		b.setState(Closed)
	case Closed:
		b.failures = 0
	}
}

func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case HalfOpen:
		b.trip()
	case Closed:
		b.failures++
		if b.failures >= b.threshold {
			b.trip()
		}
	}
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *Breaker) trip() {
	b.failures, b.probing = 0, false
	b.openedAt = b.now()
	b.setState(Open)
}

func (b *Breaker) setState(s State) {
	if b.state != s {
		b.state = s
		b.onChange(s)
	}
}
