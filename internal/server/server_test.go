package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/Bhavd33p/llm-gateway/internal/cache"
	"github.com/Bhavd33p/llm-gateway/internal/config"
	"github.com/Bhavd33p/llm-gateway/internal/cost"
	"github.com/Bhavd33p/llm-gateway/internal/metrics"
	"github.com/Bhavd33p/llm-gateway/internal/mockllm"
	"github.com/Bhavd33p/llm-gateway/internal/provider"
	"github.com/Bhavd33p/llm-gateway/internal/ratelimit"
	"github.com/Bhavd33p/llm-gateway/internal/redistest"
	"github.com/Bhavd33p/llm-gateway/internal/router"
)

const (
	testKey     = "demo-key-acme"
	testKeyHash = "8649cdebee753898fd50e01408a9e438aa5090249bc3e51b65f33dce973b0873"
)

type gateway struct {
	url      string
	mocks    []*mockllm.Server
	registry *prometheus.Registry
	metrics  *metrics.Metrics
	ledger   *cost.Ledger
	tenant   string
}

func newGateway(t *testing.T, limit config.RateLimit, mocks ...mockllm.Config) *gateway {
	t.Helper()
	rdb, _ := redistest.New(t)
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	g := &gateway{registry: reg, metrics: m}

	var upstreams []router.Upstream
	for i, cfg := range mocks {
		mock := mockllm.New(cfg)
		srv := httptest.NewServer(mock.Handler())
		t.Cleanup(srv.Close)
		g.mocks = append(g.mocks, mock)
		u := config.Upstream{Name: cfg.Name, BaseURL: srv.URL + "/v1", MaxIdleConns: 16, FirstTokenTimeout: 2 * time.Second}
		upstreams = append(upstreams, router.Upstream{
			Provider: provider.NewOpenAI(u, provider.NewHTTPClient(u)), Weight: len(mocks) - i,
			Timeout: 5 * time.Second, FirstTokenTimeout: 2 * time.Second,
		})
	}

	// A unique tenant name keeps runs against a shared real Redis independent.
	g.tenant = fmt.Sprintf("acme-%s-%d", strings.ReplaceAll(t.Name(), "/", "_"), time.Now().UnixNano())
	enabled := true
	tenants := []config.Tenant{{
		Name:         g.tenant,
		APIKeySHA256: []string{testKeyHash},
		RateLimit:    limit,
		Cache:        config.Cache{Enabled: &enabled, Threshold: 0.95, MaxEntries: 100, TTL: time.Hour},
	}}
	g.ledger = cost.NewLedger(rdb, cost.NewPricer(map[string]config.Price{"gpt-4o-mini": {InputPerMillion: 0.15, OutputPerMillion: 0.6}}), m)
	s := New(Deps{
		Tenants:      tenants,
		Router:       router.New(upstreams, router.Options{MaxAttempts: 3, Backoff: router.Backoff{Base: time.Millisecond, Max: 5 * time.Millisecond}, BreakerThreshold: 5, BreakerCooldown: time.Minute}, m),
		Cache:        cache.New(rdb, cache.NewHashEmbedder(256), m),
		Limiter:      ratelimit.New(rdb),
		Ledger:       g.ledger,
		Metrics:      m,
		Gatherer:     reg,
		Ready:        func(ctx context.Context) error { return rdb.Ping(ctx).Err() },
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxBodyBytes: 1 << 20,
	})
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	g.url = srv.URL
	return g
}

var generous = config.RateLimit{RequestsPerSecond: 1000, Burst: 1000}

func (g *gateway) post(t *testing.T, ctx context.Context, body string, headers ...string) *http.Response {
	t.Helper()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, g.url+"/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testKey)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	return resp
}

// readSSE returns the concatenated content, whether [DONE] arrived, and any in-band error message.
func readSSE(t *testing.T, body io.Reader) (content string, done bool, errMsg string) {
	t.Helper()
	sc := bufio.NewScanner(body)
	var b strings.Builder
	for sc.Scan() {
		data, ok := strings.CutPrefix(sc.Text(), "data: ")
		if !ok {
			continue
		}
		if data == "[DONE]" {
			return b.String(), true, errMsg
		}
		var chunk struct {
			provider.Chunk
			Error *struct{ Message string } `json:"error"`
		}
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			t.Fatalf("bad SSE data %q: %v", data, err)
		}
		if chunk.Error != nil {
			errMsg = chunk.Error.Message
			continue
		}
		b.WriteString(chunk.Content())
	}
	return b.String(), false, errMsg
}

func histogramCount(t *testing.T, reg *prometheus.Registry, name, label, value string) uint64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if hasLabel(m, label, value) {
				return m.GetHistogram().GetSampleCount()
			}
		}
	}
	return 0
}

func hasLabel(m *dto.Metric, name, value string) bool {
	for _, l := range m.GetLabel() {
		if l.GetName() == name && l.GetValue() == value {
			return true
		}
	}
	return false
}

const streamBody = `{"model":"gpt-4o-mini","stream":true,"messages":[{"role":"user","content":"What is the capital of France?"}]}`

func TestStreamingMissThenSemanticHit(t *testing.T) {
	g := newGateway(t, generous, mockllm.Config{Name: "mock-a", Tokens: 8})

	resp := g.post(t, context.Background(), streamBody)
	content, done, _ := readSSE(t, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("X-Cache") != "MISS" || resp.Header.Get("X-Upstream") != "mock-a" {
		t.Fatalf("status=%d X-Cache=%q X-Upstream=%q", resp.StatusCode, resp.Header.Get("X-Cache"), resp.Header.Get("X-Upstream"))
	}
	if !done || !strings.HasPrefix(content, "mock-a answers: What is the capital of France?") {
		t.Fatalf("content=%q done=%v", content, done)
	}

	// Same question, different case and spacing: a semantic hit, and the upstream is not called again.
	resp = g.post(t, context.Background(), `{"model":"gpt-4o-mini","stream":true,"messages":[{"role":"user","content":"what is the  capital of france"}]}`)
	cached, done, _ := readSSE(t, resp.Body)
	resp.Body.Close()
	if resp.Header.Get("X-Cache") != "HIT" || cached != content || !done {
		t.Fatalf("X-Cache=%q cached=%q done=%v, want a HIT replaying %q", resp.Header.Get("X-Cache"), cached, done, content)
	}
	if n := g.mocks[0].Requests(); n != 1 {
		t.Errorf("upstream saw %d requests, want 1", n)
	}

	if got := testutil.ToFloat64(g.metrics.CacheHits.WithLabelValues(g.tenant)); got != 1 {
		t.Errorf("cache hits = %v, want 1", got)
	}
	if got := histogramCount(t, g.registry, "llm_gateway_ttft_seconds", "source", "mock-a"); got != 1 {
		t.Errorf("upstream TTFT samples = %d, want 1", got)
	}
	if got := histogramCount(t, g.registry, "llm_gateway_ttft_seconds", "source", "cache"); got != 1 {
		t.Errorf("cache TTFT samples = %d, want 1", got)
	}
	if got := histogramCount(t, g.registry, "llm_gateway_upstream_request_duration_seconds", "upstream", "mock-a"); got != 1 {
		t.Errorf("upstream latency samples = %d, want 1", got)
	}
	ledger, _ := g.ledger.Month(context.Background(), g.tenant)
	if ledger["requests"] != 1 || ledger["cache_hits"] != 1 || ledger["completion_tokens"] != 8 || ledger["estimated_requests"] != 0 {
		t.Errorf("ledger = %v", ledger)
	}
}

func TestStreamIsFlushedChunkByChunk(t *testing.T) {
	g := newGateway(t, generous, mockllm.Config{Name: "slow", Tokens: 6, TokenDelay: 100 * time.Millisecond})
	start := time.Now()
	resp := g.post(t, context.Background(), streamBody, "Cache-Control", "no-cache")
	defer resp.Body.Close()

	sc := bufio.NewScanner(resp.Body)
	var firstContent time.Duration
	for sc.Scan() {
		if firstContent == 0 && strings.Contains(sc.Text(), `"content"`) {
			firstContent = time.Since(start)
		}
	}
	total := time.Since(start)
	// Six tokens 100ms apart take at least 500ms; the first must arrive long before the stream ends.
	if total < 500*time.Millisecond || firstContent > 250*time.Millisecond {
		t.Fatalf("first content after %v, stream done after %v: chunks are being buffered", firstContent, total)
	}
	if resp.Header.Get("X-Cache") != "BYPASS" {
		t.Errorf("X-Cache = %q, want BYPASS for Cache-Control: no-cache", resp.Header.Get("X-Cache"))
	}
}

func TestFailoverToHealthyUpstream(t *testing.T) {
	g := newGateway(t, generous,
		mockllm.Config{Name: "broken", FailureRate: 1},
		mockllm.Config{Name: "healthy", Tokens: 4},
	)
	resp := g.post(t, context.Background(), streamBody)
	content, done, _ := readSSE(t, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("X-Upstream") != "healthy" || !done || !strings.HasPrefix(content, "healthy") {
		t.Fatalf("status=%d upstream=%q content=%q", resp.StatusCode, resp.Header.Get("X-Upstream"), content)
	}
	if got := testutil.ToFloat64(g.metrics.Failovers.WithLabelValues("broken", "healthy")); got != 1 {
		t.Errorf("failovers = %v, want 1", got)
	}
}

func TestAllUpstreamsDownReturns502(t *testing.T) {
	g := newGateway(t, generous, mockllm.Config{Name: "a", FailureRate: 1}, mockllm.Config{Name: "b", FailureRate: 1})
	resp := g.post(t, context.Background(), streamBody)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
}

func TestMidStreamFailureIsReportedNotRetriedNorCached(t *testing.T) {
	g := newGateway(t, generous,
		mockllm.Config{Name: "flaky", Tokens: 5, MidStreamFailureRate: 1},
		mockllm.Config{Name: "healthy", Tokens: 5},
	)
	resp := g.post(t, context.Background(), streamBody)
	content, done, errMsg := readSSE(t, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || done || errMsg == "" || content != "flaky" {
		t.Fatalf("content=%q done=%v err=%q: want the first token, then an in-band error and no [DONE]", content, done, errMsg)
	}
	if g.mocks[1].Requests() != 0 {
		t.Error("after the first byte the gateway must not retry on another upstream")
	}
	resp = g.post(t, context.Background(), streamBody)
	resp.Body.Close()
	if resp.Header.Get("X-Cache") != "MISS" {
		t.Errorf("a broken answer must not be cached, X-Cache = %q", resp.Header.Get("X-Cache"))
	}
}

func TestClientDisconnectCancelsUpstream(t *testing.T) {
	g := newGateway(t, generous, mockllm.Config{Name: "slow", Tokens: 100, TokenDelay: 50 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	resp := g.post(t, ctx, streamBody)

	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() && !strings.Contains(sc.Text(), `"content"`) {
	}
	cancel() // hang up after the first token
	resp.Body.Close()

	deadline := time.Now().Add(2 * time.Second)
	for g.mocks[0].Cancelled() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if g.mocks[0].Cancelled() != 1 {
		t.Fatal("the upstream request was not cancelled after the client disconnected")
	}
}

func TestNonStreamingMissThenHit(t *testing.T) {
	g := newGateway(t, generous, mockllm.Config{Name: "mock-a", Tokens: 5})
	body := `{"model":"gpt-4o-mini","messages":[{"role":"user","content":"Name a prime number"}]}`
	for i, want := range []string{"MISS", "HIT"} {
		resp := g.post(t, context.Background(), body)
		var out provider.ChatResponse
		json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if resp.StatusCode != 200 || resp.Header.Get("X-Cache") != want || len(out.Choices) != 1 || out.Choices[0].Message.Content == "" {
			t.Fatalf("request %d: status=%d X-Cache=%q response=%+v", i, resp.StatusCode, resp.Header.Get("X-Cache"), out)
		}
	}
}

func TestRateLimitReturns429WithRetryAfter(t *testing.T) {
	g := newGateway(t, config.RateLimit{RequestsPerSecond: 0.5, Burst: 1}, mockllm.Config{Name: "mock-a", Tokens: 2})
	first := g.post(t, context.Background(), streamBody)
	io.Copy(io.Discard, first.Body)
	first.Body.Close()

	second := g.post(t, context.Background(), streamBody)
	second.Body.Close()
	if second.StatusCode != http.StatusTooManyRequests || second.Header.Get("Retry-After") != "2" {
		t.Fatalf("status=%d Retry-After=%q, want 429 with Retry-After 2 at 0.5 req/s", second.StatusCode, second.Header.Get("Retry-After"))
	}
	if got := testutil.ToFloat64(g.metrics.RateLimited.WithLabelValues(g.tenant)); got != 1 {
		t.Errorf("rate limited metric = %v, want 1", got)
	}
}

func TestRejectsMissingOrUnknownKey(t *testing.T) {
	g := newGateway(t, generous, mockllm.Config{Name: "mock-a"})
	for _, auth := range []string{"", "Bearer wrong-key"} {
		req, _ := http.NewRequest(http.MethodPost, g.url+"/v1/chat/completions", strings.NewReader(streamBody))
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("auth %q: status %d, want 401", auth, resp.StatusCode)
		}
	}
}

func TestUsageAndMetricsEndpoints(t *testing.T) {
	g := newGateway(t, generous, mockllm.Config{Name: "mock-a", Tokens: 3})
	resp := g.post(t, context.Background(), streamBody)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	req, _ := http.NewRequest(http.MethodGet, g.url+"/v1/usage", nil)
	req.Header.Set("X-API-Key", testKey)
	usage, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Tenant string           `json:"tenant"`
		Ledger map[string]int64 `json:"ledger"`
	}
	json.NewDecoder(usage.Body).Decode(&body)
	usage.Body.Close()
	if body.Tenant != g.tenant || body.Ledger["requests"] != 1 {
		t.Errorf("usage = %+v", body)
	}

	metricsResp, err := http.Get(g.url + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	text, _ := io.ReadAll(metricsResp.Body)
	metricsResp.Body.Close()
	for _, name := range []string{"llm_gateway_cache_misses_total", "llm_gateway_ttft_seconds_bucket", "llm_gateway_upstream_request_duration_seconds_bucket", "llm_gateway_tenant_spend_usd_total"} {
		if !strings.Contains(string(text), name) {
			t.Errorf("/metrics is missing %s", name)
		}
	}
}
