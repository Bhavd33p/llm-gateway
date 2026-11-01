package router

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Bhavd33p/llm-gateway/internal/metrics"
	"github.com/Bhavd33p/llm-gateway/internal/provider"
)

// fakeProvider is an in-memory upstream whose behaviour each test scripts.
type fakeProvider struct {
	name     string
	calls    atomic.Int32
	complete func(ctx context.Context) (*provider.ChatResponse, error)
	stream   func(ctx context.Context) (provider.Stream, error)
}

func (f *fakeProvider) Name() string             { return f.name }
func (f *fakeProvider) ModelFor(m string) string { return m }
func (f *fakeProvider) Complete(ctx context.Context, _ *provider.ChatRequest) (*provider.ChatResponse, error) {
	f.calls.Add(1)
	return f.complete(ctx)
}
func (f *fakeProvider) Stream(ctx context.Context, _ *provider.ChatRequest) (provider.Stream, error) {
	f.calls.Add(1)
	return f.stream(ctx)
}

func ok(name string) *fakeProvider {
	return &fakeProvider{
		name: name,
		complete: func(context.Context) (*provider.ChatResponse, error) {
			return &provider.ChatResponse{Model: name}, nil
		},
		stream: func(ctx context.Context) (provider.Stream, error) {
			return &fakeStream{ctx: ctx, chunks: []string{"", "hello from " + name}}, nil
		},
	}
}

func failing(name string, err error) *fakeProvider {
	return &fakeProvider{
		name:     name,
		complete: func(context.Context) (*provider.ChatResponse, error) { return nil, err },
		stream:   func(context.Context) (provider.Stream, error) { return nil, err },
	}
}

// fakeStream yields one chunk per string (an empty string is a role-only chunk), then failAfter or io.EOF.
// With block set, Recv waits for the context instead, like an upstream that never sends a token.
type fakeStream struct {
	ctx       context.Context
	chunks    []string
	failAfter error
	block     bool
	i         int
}

func (s *fakeStream) Recv() (*provider.Chunk, error) {
	if s.block {
		<-s.ctx.Done()
		return nil, s.ctx.Err()
	}
	if s.i < len(s.chunks) {
		c := &provider.Chunk{Choices: []provider.ChunkChoice{{Delta: provider.Delta{Content: s.chunks[s.i]}}}}
		s.i++
		return c, nil
	}
	if s.failAfter != nil {
		return nil, s.failAfter
	}
	return nil, io.EOF
}

func (s *fakeStream) Close() error { return nil }

var errServer = &provider.StatusError{Provider: "x", StatusCode: 503, Body: "unavailable"}

func newRouter(t *testing.T, opts Options, providers ...*fakeProvider) (*Router, *metrics.Metrics) {
	t.Helper()
	m := metrics.New(prometheus.NewRegistry())
	if opts.MaxAttempts == 0 {
		opts.MaxAttempts = 3
	}
	if opts.BreakerThreshold == 0 {
		opts.BreakerThreshold = 100
	}
	if opts.BreakerCooldown == 0 {
		opts.BreakerCooldown = time.Minute
	}
	opts.Backoff = Backoff{Base: time.Millisecond, Max: 2 * time.Millisecond}
	var ups []Upstream
	for i, p := range providers {
		ups = append(ups, Upstream{Provider: p, Weight: len(providers) - i, Timeout: time.Second, FirstTokenTimeout: time.Second})
	}
	return New(ups, opts, m), m
}

func readAll(t *testing.T, s provider.Stream) (string, error) {
	t.Helper()
	defer s.Close()
	var b strings.Builder
	for {
		c, err := s.Recv()
		if err == io.EOF {
			return b.String(), nil
		}
		if err != nil {
			return b.String(), err
		}
		b.WriteString(c.Content())
	}
}

func TestSmoothWeightedRoundRobin(t *testing.T) {
	m := metrics.New(prometheus.NewRegistry())
	r := New([]Upstream{
		{Provider: ok("a"), Weight: 5},
		{Provider: ok("b"), Weight: 1},
		{Provider: ok("c"), Weight: 1},
	}, Options{MaxAttempts: 1}, m)

	var seq []string
	for range 7 {
		seq = append(seq, r.pick().name)
	}
	if got := strings.Join(seq, " "); got != "a a b a c a a" {
		t.Errorf("first cycle = %q, want the interleaved %q", got, "a a b a c a a")
	}

	counts := map[string]int{}
	for range 7000 {
		counts[r.pick().name]++
	}
	if counts["a"] != 5000 || counts["b"] != 1000 || counts["c"] != 1000 {
		t.Errorf("counts = %v, want exactly 5000/1000/1000", counts)
	}
}

func TestFailoverToNextUpstreamOn5xx(t *testing.T) {
	bad, good := failing("bad", errServer), ok("good")
	r, m := newRouter(t, Options{}, bad, good)

	resp, served, err := r.Complete(context.Background(), &provider.ChatRequest{})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if resp.Model != "good" || served.Upstream != "good" || served.Attempts != 2 {
		t.Errorf("served = %+v, want good after 2 attempts", served)
	}
	if got := testutil.ToFloat64(m.Failovers.WithLabelValues("bad", "good")); got != 1 {
		t.Errorf("failovers metric = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.UpstreamErrors.WithLabelValues("bad", "status_5xx")); got != 1 {
		t.Errorf("upstream errors metric = %v, want 1", got)
	}
}

func TestClientErrorIsNotRetried(t *testing.T) {
	bad := failing("bad", &provider.StatusError{StatusCode: 400, Body: "bad request"})
	good := ok("good")
	r, _ := newRouter(t, Options{}, bad, good)

	_, served, err := r.Complete(context.Background(), &provider.ChatRequest{})
	var se *provider.StatusError
	if !errors.As(err, &se) || se.StatusCode != 400 {
		t.Fatalf("err = %v, want the upstream's 400", err)
	}
	if served.Attempts != 1 || good.calls.Load() != 0 {
		t.Errorf("a 400 must not be retried: attempts=%d good calls=%d", served.Attempts, good.calls.Load())
	}
}

func TestRetriesSameUpstreamWhenItIsTheOnlyOne(t *testing.T) {
	bad := failing("only", errServer)
	r, _ := newRouter(t, Options{MaxAttempts: 4}, bad)
	_, served, err := r.Complete(context.Background(), &provider.ChatRequest{})
	if err == nil || served.Attempts != 4 || bad.calls.Load() != 4 {
		t.Fatalf("err=%v attempts=%d calls=%d, want 4 failed attempts", err, served.Attempts, bad.calls.Load())
	}
}

func TestOpenBreakerSkipsUpstream(t *testing.T) {
	bad, good := failing("bad", errServer), ok("good")
	r, m := newRouter(t, Options{BreakerThreshold: 2}, bad, good)

	for range 6 {
		if _, _, err := r.Complete(context.Background(), &provider.ChatRequest{}); err != nil {
			t.Fatalf("Complete: %v", err)
		}
	}
	if got := bad.calls.Load(); got != 2 {
		t.Errorf("bad upstream called %d times, want 2 (then its breaker opens)", got)
	}
	if got := testutil.ToFloat64(m.BreakerState.WithLabelValues("bad")); got != float64(Open) {
		t.Errorf("breaker gauge = %v, want %v (open)", got, float64(Open))
	}
}

func TestAllBreakersOpen(t *testing.T) {
	r, _ := newRouter(t, Options{BreakerThreshold: 1, MaxAttempts: 1}, failing("a", errServer), failing("b", errServer))
	r.Complete(context.Background(), &provider.ChatRequest{}) // opens one breaker
	r.Complete(context.Background(), &provider.ChatRequest{}) // opens the other
	if _, _, err := r.Complete(context.Background(), &provider.ChatRequest{}); !errors.Is(err, ErrNoHealthyUpstream) {
		t.Fatalf("err = %v, want ErrNoHealthyUpstream", err)
	}
}

func TestStreamRetriesWhenUpstreamFailsBeforeFirstToken(t *testing.T) {
	flaky := &fakeProvider{name: "flaky", stream: func(ctx context.Context) (provider.Stream, error) {
		// HTTP 200, a role chunk, then the connection drops before any content.
		return &fakeStream{ctx: ctx, chunks: []string{""}, failAfter: io.ErrUnexpectedEOF}, nil
	}}
	good := ok("good")
	r, _ := newRouter(t, Options{}, flaky, good)

	stream, served, err := r.Stream(context.Background(), &provider.ChatRequest{})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	text, err := readAll(t, stream)
	if err != nil || text != "hello from good" || served.Upstream != "good" || served.Attempts != 2 {
		t.Fatalf("text=%q served=%+v err=%v, want failover to good", text, served, err)
	}
}

func TestStreamDoesNotRetryAfterFirstToken(t *testing.T) {
	midway := &fakeProvider{name: "midway", stream: func(ctx context.Context) (provider.Stream, error) {
		return &fakeStream{ctx: ctx, chunks: []string{"partial answer"}, failAfter: io.ErrUnexpectedEOF}, nil
	}}
	good := ok("good")
	r, m := newRouter(t, Options{}, midway, good)

	stream, served, err := r.Stream(context.Background(), &provider.ChatRequest{})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	text, err := readAll(t, stream)
	if !errors.Is(err, io.ErrUnexpectedEOF) || text != "partial answer" {
		t.Fatalf("text=%q err=%v, want the partial answer then the mid-stream error", text, err)
	}
	if served.Upstream != "midway" || good.calls.Load() != 0 {
		t.Errorf("after the first token the router must not fail over (good calls=%d)", good.calls.Load())
	}
	if got := testutil.ToFloat64(m.UpstreamErrors.WithLabelValues("midway", "stream")); got != 1 {
		t.Errorf("stream error metric = %v, want 1", got)
	}
}

func TestStreamFirstTokenTimeoutFailsOver(t *testing.T) {
	stuck := &fakeProvider{name: "stuck", stream: func(ctx context.Context) (provider.Stream, error) {
		return &fakeStream{ctx: ctx, block: true}, nil
	}}
	good := ok("good")
	m := metrics.New(prometheus.NewRegistry())
	r := New([]Upstream{
		{Provider: stuck, Weight: 2, FirstTokenTimeout: 20 * time.Millisecond},
		{Provider: good, Weight: 1, FirstTokenTimeout: time.Second},
	}, Options{MaxAttempts: 2, BreakerThreshold: 5, BreakerCooldown: time.Minute, Backoff: Backoff{Base: time.Millisecond, Max: time.Millisecond}}, m)

	stream, served, err := r.Stream(context.Background(), &provider.ChatRequest{})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	stream.Close()
	if served.Upstream != "good" {
		t.Errorf("served by %q, want good", served.Upstream)
	}
	if got := testutil.ToFloat64(m.UpstreamErrors.WithLabelValues("stuck", "timeout")); got != 1 {
		t.Errorf("timeout metric = %v, want 1", got)
	}
}

func TestClientCancellationStopsRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stuck := &fakeProvider{name: "stuck", stream: func(ctx context.Context) (provider.Stream, error) {
		cancel() // the client hangs up while we wait for the first token
		return &fakeStream{ctx: ctx, block: true}, nil
	}}
	good := ok("good")
	r, _ := newRouter(t, Options{}, stuck, good)

	_, _, err := r.Stream(ctx, &provider.ChatRequest{})
	if !errors.Is(err, context.Canceled) || good.calls.Load() != 0 {
		t.Fatalf("err=%v good calls=%d, want context.Canceled and no failover", err, good.calls.Load())
	}
}
