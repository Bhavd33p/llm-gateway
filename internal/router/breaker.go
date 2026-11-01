package router

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

// Breaker is a consecutive-failure circuit breaker. It stops the router from spending attempts (and the
// client's latency budget) on an upstream that is clearly down, and lets a single probe test it after a cooldown.
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

func NewBreaker(threshold int, cooldown time.Duration, onChange func(State)) *Breaker {
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

func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures, b.probing = 0, false
	b.setState(Closed)
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
