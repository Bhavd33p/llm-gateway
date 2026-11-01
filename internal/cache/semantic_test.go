package cache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"

	"github.com/Bhavd33p/llm-gateway/internal/config"
	"github.com/Bhavd33p/llm-gateway/internal/metrics"
	"github.com/Bhavd33p/llm-gateway/internal/provider"
	"github.com/Bhavd33p/llm-gateway/internal/redistest"
)

func TestPromptTextNormalises(t *testing.T) {
	a := PromptText("m", []provider.Message{{Role: "user", Content: "  Hello\n\tWORLD  "}})
	b := PromptText("m", []provider.Message{{Role: "user", Content: "hello world"}})
	if a != b || a != "model: m\nuser: hello world" {
		t.Errorf("PromptText = %q and %q", a, b)
	}
}

type testCache struct {
	*Cache
	metrics *metrics.Metrics
	clock   time.Time
}

func newTestCache(t *testing.T) *testCache {
	t.Helper()
	rdb, _ := redistest.New(t)
	m := metrics.New(prometheus.NewRegistry())
	tc := &testCache{Cache: New(rdb, NewHashEmbedder(256), m), metrics: m, clock: time.Unix(1_700_000_000, 0)}
	tc.now = func() time.Time {
		tc.clock = tc.clock.Add(time.Millisecond) // strictly increasing access times
		return tc.clock
	}
	return tc
}

var settings = config.Cache{Threshold: 0.95, MaxEntries: 100, TTL: time.Hour}

func (tc *testCache) store(t *testing.T, tenant, model, prompt string, s config.Cache) {
	t.Helper()
	msgs := []provider.Message{{Role: "user", Content: prompt}}
	vec, _ := tc.Embed(context.Background(), model, msgs)
	if err := tc.Store(context.Background(), tenant, model, "", vec, Entry{Content: "answer to " + prompt, FinishReason: "stop"}, s); err != nil {
		t.Fatalf("Store: %v", err)
	}
}

func (tc *testCache) lookup(t *testing.T, tenant, model, prompt string, s config.Cache) *Hit {
	t.Helper()
	msgs := []provider.Message{{Role: "user", Content: prompt}}
	vec, _ := tc.Embed(context.Background(), model, msgs)
	hit, err := tc.Lookup(context.Background(), tenant, model, "", vec, s)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	return hit
}

func uniqueTenant() string { return fmt.Sprintf("t%d", time.Now().UnixNano()) }

func TestCacheHitAndMiss(t *testing.T) {
	tc := newTestCache(t)
	tenant := uniqueTenant()
	tc.store(t, tenant, "gpt-4o-mini", "What is the capital of France?", settings)

	hit := tc.lookup(t, tenant, "gpt-4o-mini", "what is the capital of france", settings)
	if hit == nil || hit.Entry.Content != "answer to What is the capital of France?" || hit.Similarity < 0.999 {
		t.Fatalf("expected a hit for the same question, got %+v", hit)
	}
	if hit := tc.lookup(t, tenant, "gpt-4o-mini", "explain the raft consensus algorithm", settings); hit != nil {
		t.Errorf("unrelated prompt should miss, got similarity %v", hit.Similarity)
	}
	if hit := tc.lookup(t, tenant, "claude-haiku-4-5", "What is the capital of France?", settings); hit != nil {
		t.Error("the same prompt for a different model must miss")
	}
	if hit := tc.lookup(t, uniqueTenant()+"x", "gpt-4o-mini", "What is the capital of France?", settings); hit != nil {
		t.Error("another tenant must not see this tenant's cache")
	}

	if hits := testutil.ToFloat64(tc.metrics.CacheHits.WithLabelValues(tenant)); hits != 1 {
		t.Errorf("hits = %v, want 1", hits)
	}
	if misses := testutil.ToFloat64(tc.metrics.CacheMisses.WithLabelValues(tenant)); misses != 2 {
		t.Errorf("misses = %v, want 2", misses)
	}
}

func TestCacheThreshold(t *testing.T) {
	tc := newTestCache(t)
	tenant := uniqueTenant()
	tc.store(t, tenant, "m", "what is the capital of france", settings)
	strict := settings
	strict.Threshold = 0.99
	if hit := tc.lookup(t, tenant, "m", "what is the capital city of france", strict); hit != nil {
		t.Errorf("similarity %v should not pass a 0.99 threshold", hit.Similarity)
	}
	loose := settings
	loose.Threshold = 0.7
	if hit := tc.lookup(t, tenant, "m", "what is the capital city of france", loose); hit == nil {
		t.Error("a near-duplicate should pass a 0.7 threshold")
	}
}

func TestCacheKeyLayout(t *testing.T) {
	tc := newTestCache(t)
	tenant := uniqueTenant()
	tc.store(t, tenant, "m", "hello", settings)

	ctx := context.Background()
	ids := tc.rdb.ZRange(ctx, "sc:{"+tenant+"}:lru", 0, -1).Val()
	if len(ids) != 1 {
		t.Fatalf("LRU index has %d members, want 1", len(ids))
	}
	fields := tc.rdb.HGetAll(ctx, "sc:{"+tenant+"}:e:"+ids[0]).Val()
	if _, ok := fields["params"]; !ok || len(fields["emb"]) != 256*4 || fields["model"] != "m" || fields["created_at"] == "" || fields["resp"] == "" {
		t.Errorf("entry hash fields = %v", fields)
	}
	if ttl := tc.rdb.TTL(ctx, "sc:{"+tenant+"}:e:"+ids[0]).Val(); ttl <= 0 || ttl > time.Hour {
		t.Errorf("entry TTL = %v, want (0, 1h]", ttl)
	}
}

func TestLRUEviction(t *testing.T) {
	tc := newTestCache(t)
	tenant := uniqueTenant()
	small := settings
	small.MaxEntries = 2

	tc.store(t, tenant, "m", "first prompt about apples", small)
	tc.store(t, tenant, "m", "second prompt about bananas", small)
	if tc.lookup(t, tenant, "m", "first prompt about apples", small) == nil { // bumps "first" to most recent
		t.Fatal("first prompt should be cached")
	}
	tc.store(t, tenant, "m", "third prompt about cherries", small) // evicts the least recently used: "second"

	if tc.lookup(t, tenant, "m", "second prompt about bananas", small) != nil {
		t.Error("second prompt was least recently used and should have been evicted")
	}
	for _, p := range []string{"first prompt about apples", "third prompt about cherries"} {
		if tc.lookup(t, tenant, "m", p, small) == nil {
			t.Errorf("%q should still be cached", p)
		}
	}
	if n := tc.rdb.ZCard(context.Background(), "sc:{"+tenant+"}:lru").Val(); n != 2 {
		t.Errorf("LRU index size = %d, want 2", n)
	}
	if got := testutil.ToFloat64(tc.metrics.CacheEvictions.WithLabelValues(tenant)); got != 1 {
		t.Errorf("evictions metric = %v, want 1", got)
	}
}

func TestExpiredEntriesAreSkippedAndPruned(t *testing.T) {
	rdb, mini := redistest.NewMini(t)
	c := New(rdb, NewHashEmbedder(64), metrics.New(prometheus.NewRegistry()))
	ctx := context.Background()
	msgs := []provider.Message{{Role: "user", Content: "hello"}}
	vec, _ := c.Embed(ctx, "m", msgs)
	short := config.Cache{Threshold: 0.95, MaxEntries: 10, TTL: time.Minute}

	if err := c.Store(ctx, "acme", "m", "", vec, Entry{Content: "hi"}, short); err != nil {
		t.Fatal(err)
	}
	// Entry expires; recreate the index member as if it had outlived the entry.
	ids := rdb.ZRange(ctx, lruKey("acme"), 0, -1).Val()
	mini.FastForward(2 * time.Minute)
	rdb.ZAdd(ctx, lruKey("acme"), redis.Z{Score: 1, Member: ids[0]})

	hit, err := c.Lookup(ctx, "acme", "m", "", vec, short)
	if err != nil || hit != nil {
		t.Fatalf("expired entry must not be served: hit=%v err=%v", hit, err)
	}
	if n := rdb.ZCard(ctx, lruKey("acme")).Val(); n != 0 {
		t.Errorf("stale index member should be pruned, %d left", n)
	}
}

func TestCacheRequiresSameParameters(t *testing.T) {
	tc := newTestCache(t)
	tenant := uniqueTenant()
	ctx := context.Background()
	msgs := []provider.Message{{Role: "user", Content: "write a long essay about redis"}}
	vec, _ := tc.Embed(ctx, "m", msgs)
	long, short := 500, 5
	longKey := ParamsKey(&provider.ChatRequest{MaxTokens: &long})
	shortKey := ParamsKey(&provider.ChatRequest{MaxTokens: &short})
	if longKey == shortKey || ParamsKey(&provider.ChatRequest{}) == longKey {
		t.Fatalf("parameter fingerprints should differ: %q %q", longKey, shortKey)
	}

	if err := tc.Store(ctx, tenant, "m", longKey, vec, Entry{Content: "a very long essay"}, settings); err != nil {
		t.Fatal(err)
	}
	if hit, _ := tc.Lookup(ctx, tenant, "m", shortKey, vec, settings); hit != nil {
		t.Error("an answer generated with max_tokens=500 must not be served to a max_tokens=5 request")
	}
	if hit, _ := tc.Lookup(ctx, tenant, "m", longKey, vec, settings); hit == nil {
		t.Error("the same parameters should hit")
	}
}
