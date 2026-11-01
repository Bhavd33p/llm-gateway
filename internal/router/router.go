// Package router picks an upstream for each request, fails over between upstreams, and retries with backoff.
package router

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/Bhavd33p/llm-gateway/internal/metrics"
	"github.com/Bhavd33p/llm-gateway/internal/provider"
)

// maxBufferedChunks caps read-ahead while waiting for the first token, in case an upstream sends many empty chunks.
const maxBufferedChunks = 16

var (
	ErrNoHealthyUpstream = errors.New("no healthy upstream: every circuit breaker is open")
	errFirstTokenTimeout = errors.New("timed out waiting for the first token")
	errEmptyStream       = errors.New("stream ended before the first token")
)

type Upstream struct {
	Provider          provider.Provider
	Weight            int
	Timeout           time.Duration
	FirstTokenTimeout time.Duration
}

type Options struct {
	MaxAttempts      int
	Backoff          Backoff
	BreakerThreshold int
	BreakerCooldown  time.Duration
}

// Served describes which upstream answered and how many attempts it took.
type Served struct {
	Upstream string
	Model    string
	Attempts int
}

type upstream struct {
	Upstream
	name    string
	breaker *Breaker
	current int // smooth weighted round-robin score, guarded by Router.mu
}

type Router struct {
	mu          sync.Mutex
	upstreams   []*upstream
	byWeight    []*upstream
	totalWeight int
	maxAttempts int
	backoff     Backoff
	metrics     *metrics.Metrics
}

func New(upstreams []Upstream, opts Options, m *metrics.Metrics) *Router {
	r := &Router{maxAttempts: max(1, opts.MaxAttempts), backoff: opts.Backoff, metrics: m}
	for _, u := range upstreams {
		name := u.Provider.Name()
		gauge := m.BreakerState.WithLabelValues(name)
		gauge.Set(float64(Closed))
		r.upstreams = append(r.upstreams, &upstream{
			Upstream: u,
			name:     name,
			breaker:  NewBreaker(opts.BreakerThreshold, opts.BreakerCooldown, func(s State) { gauge.Set(float64(s)) }),
		})
		r.totalWeight += u.Weight
	}
	r.byWeight = slices.Clone(r.upstreams)
	slices.SortStableFunc(r.byWeight, func(a, b *upstream) int { return b.Weight - a.Weight })
	return r
}

// pick is nginx's smooth weighted round-robin. Each call adds every upstream's weight to its score, chooses the
// highest score and subtracts the total weight from the winner. Over any window of totalWeight picks each upstream
// is chosen exactly in proportion to its weight, and picks are interleaved (weights 5:1:1 give a a b a c a a)
// instead of arriving in bursts the way plain round-robin over a weighted list would.
func (r *Router) pick() *upstream {
	r.mu.Lock()
	defer r.mu.Unlock()
	var best *upstream
	for _, u := range r.upstreams {
		u.current += u.Weight
		if best == nil || u.current > best.current {
			best = u
		}
	}
	best.current -= r.totalWeight
	return best
}

// candidates returns the weighted pick first, then every other upstream by descending weight as failover order.
func (r *Router) candidates() []*upstream {
	primary := r.pick()
	out := make([]*upstream, 0, len(r.upstreams))
	out = append(out, primary)
	for _, u := range r.byWeight {
		if u != primary {
			out = append(out, u)
		}
	}
	return out
}

// try runs attempt against upstreams in failover order until one succeeds, a non-retryable error occurs, the
// attempt budget is spent or the client goes away. Upstreams whose breaker is open are skipped without spending
// an attempt. Every retry, to the same or to another upstream, first sleeps a full-jitter backoff.
func (r *Router) try(ctx context.Context, attempt func(context.Context, *upstream) error) (*upstream, int, error) {
	candidates := r.candidates()
	var (
		lastErr  error
		previous *upstream
		attempts int
		skipped  int
	)
	for i := 0; attempts < r.maxAttempts; i++ {
		u := candidates[i%len(candidates)]
		if !u.breaker.Allow() {
			if skipped++; skipped == len(candidates) {
				break
			}
			continue
		}
		skipped = 0

		if attempts > 0 {
			if err := sleep(ctx, r.backoff.Delay(attempts-1)); err != nil {
				return nil, attempts, err
			}
			if u != previous {
				r.metrics.Failovers.WithLabelValues(previous.name, u.name).Inc()
			}
		}
		attempts++
		start := time.Now()
		err := attempt(ctx, u)
		if err == nil {
			u.breaker.Success()
			return u, attempts, nil
		}
		if ctx.Err() != nil {
			return nil, attempts, ctx.Err() // the client left; that says nothing about the upstream
		}

		reason, retryable := classify(err)
		r.metrics.UpstreamErrors.WithLabelValues(u.name, reason).Inc()
		r.metrics.UpstreamDuration.WithLabelValues(u.name, "error").Observe(time.Since(start).Seconds())
		if !retryable {
			u.breaker.Success() // the upstream answered coherently; the request itself is bad
			return nil, attempts, err
		}
		u.breaker.Failure()
		lastErr, previous = err, u
	}
	if lastErr == nil {
		return nil, attempts, ErrNoHealthyUpstream
	}
	return nil, attempts, fmt.Errorf("all %d attempts failed, last error: %w", attempts, lastErr)
}

func (r *Router) Complete(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, Served, error) {
	var resp *provider.ChatResponse
	u, attempts, err := r.try(ctx, func(ctx context.Context, u *upstream) error {
		ctx, cancel := context.WithTimeout(ctx, u.Timeout)
		defer cancel()
		start := time.Now()
		out, err := u.Provider.Complete(ctx, req)
		if err != nil {
			return err
		}
		r.metrics.UpstreamDuration.WithLabelValues(u.name, "ok").Observe(time.Since(start).Seconds())
		resp = out
		return nil
	})
	if err != nil {
		return nil, Served{Attempts: attempts}, err
	}
	return resp, Served{Upstream: u.name, Model: u.Provider.ModelFor(req.Model), Attempts: attempts}, nil
}

// Stream returns a stream that has already produced its first token. Retries and failover happen only up to that
// point: once the caller starts writing chunks to the client, a different upstream would produce a different
// answer, and the client cannot un-read the half it already has.
func (r *Router) Stream(ctx context.Context, req *provider.ChatRequest) (provider.Stream, Served, error) {
	var stream *routedStream
	u, attempts, err := r.try(ctx, func(ctx context.Context, u *upstream) error {
		s, err := r.openStream(ctx, u, req)
		stream = s
		return err
	})
	if err != nil {
		return nil, Served{Attempts: attempts}, err
	}
	return stream, Served{Upstream: u.name, Model: u.Provider.ModelFor(req.Model), Attempts: attempts}, nil
}

// openStream starts a stream and reads ahead to the first chunk that carries content. Nothing has reached the
// client yet, so any failure here is still safe to retry elsewhere.
func (r *Router) openStream(ctx context.Context, u *upstream, req *provider.ChatRequest) (*routedStream, error) {
	start := time.Now()
	attemptCtx, cancel := context.WithCancelCause(ctx)
	timer := time.AfterFunc(u.FirstTokenTimeout, func() { cancel(errFirstTokenTimeout) })
	fail := func(err error) (*routedStream, error) {
		timer.Stop()
		if errors.Is(context.Cause(attemptCtx), errFirstTokenTimeout) {
			err = errFirstTokenTimeout
		}
		cancel(nil)
		return nil, err
	}

	s, err := u.Provider.Stream(attemptCtx, req)
	if err != nil {
		return fail(err)
	}
	var buffered []*provider.Chunk
	for {
		c, err := s.Recv()
		if err != nil {
			s.Close()
			if err == io.EOF {
				err = errEmptyStream
			}
			return fail(err)
		}
		buffered = append(buffered, c)
		if c.Content() != "" || c.FinishReason() != "" || len(buffered) >= maxBufferedChunks {
			break
		}
	}
	if !timer.Stop() { // the deadline fired just as the token arrived; the attempt context is already cancelled
		s.Close()
		return fail(errFirstTokenTimeout)
	}
	return &routedStream{stream: s, buffered: buffered, clientCtx: ctx, cancel: cancel, router: r, upstream: u, start: start}, nil
}

// routedStream replays the read-ahead chunks, then reads the upstream directly. It reports the attempt's latency
// and, for failures after the first token, marks the upstream unhealthy without retrying.
type routedStream struct {
	stream    provider.Stream
	buffered  []*provider.Chunk
	clientCtx context.Context
	cancel    context.CancelCauseFunc
	router    *Router
	upstream  *upstream
	start     time.Time
	done      bool
}

func (s *routedStream) Recv() (*provider.Chunk, error) {
	if len(s.buffered) > 0 {
		c := s.buffered[0]
		s.buffered = s.buffered[1:]
		return c, nil
	}
	c, err := s.stream.Recv()
	switch {
	case err == nil:
	case err == io.EOF:
		s.finish("ok")
	case s.clientCtx.Err() != nil:
		s.finish("cancelled")
	default:
		s.finish("error")
		s.upstream.breaker.Failure()
		s.router.metrics.UpstreamErrors.WithLabelValues(s.upstream.name, "stream").Inc()
	}
	return c, err
}

func (s *routedStream) Close() error {
	s.finish("cancelled") // no-op if the stream already ended
	s.cancel(nil)
	return s.stream.Close()
}

func (s *routedStream) finish(outcome string) {
	if s.done {
		return
	}
	s.done = true
	s.router.metrics.UpstreamDuration.WithLabelValues(s.upstream.name, outcome).Observe(time.Since(s.start).Seconds())
}

// classify labels an attempt's error for metrics and decides whether another attempt could succeed.
// 4xx (except 429) is not retried: the same request would be rejected by any upstream.
func classify(err error) (reason string, retryable bool) {
	var statusErr *provider.StatusError
	var netErr net.Error
	switch {
	case errors.As(err, &statusErr):
		switch {
		case statusErr.StatusCode == http.StatusTooManyRequests:
			return "status_429", true
		case statusErr.StatusCode >= 500:
			return "status_5xx", true
		default:
			return "status_4xx", false
		}
	case errors.Is(err, errFirstTokenTimeout), errors.Is(err, context.DeadlineExceeded):
		return "timeout", true
	case errors.As(err, &netErr):
		if netErr.Timeout() {
			return "timeout", true
		}
		return "connect", true
	case errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, errEmptyStream):
		return "stream", true
	default:
		return "protocol", true
	}
}
