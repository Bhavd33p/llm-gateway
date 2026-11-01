package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Bhavd33p/llm-gateway/internal/config"
	"github.com/Bhavd33p/llm-gateway/internal/redistest"
)

func uniqueTenant(t *testing.T) string {
	return "test-" + time.Now().Format("150405.000000000")
}

func TestBurstThenReject(t *testing.T) {
	rdb, _ := redistest.New(t)
	l, tenant := New(rdb), uniqueTenant(t)
	limit := config.RateLimit{RequestsPerSecond: 1, Burst: 3}

	for i := range 3 {
		res, err := l.Allow(context.Background(), tenant, limit)
		if err != nil || !res.Allowed {
			t.Fatalf("request %d: allowed=%v err=%v, want allowed within the burst", i, res.Allowed, err)
		}
	}
	res, err := l.Allow(context.Background(), tenant, limit)
	if err != nil || res.Allowed {
		t.Fatalf("4th request: allowed=%v err=%v, want rejected", res.Allowed, err)
	}
	if res.RetryAfter <= 0 || res.RetryAfter > time.Second {
		t.Errorf("RetryAfter = %v, want (0, 1s] at 1 token/s", res.RetryAfter)
	}
	ttl := rdb.PTTL(context.Background(), Key(tenant)).Val()
	if ttl <= 0 || ttl > 4*time.Second {
		t.Errorf("bucket TTL = %v, want about capacity/rate + 1s", ttl)
	}
}

func TestRefillUsesRedisTime(t *testing.T) {
	rdb, mini := redistest.NewMini(t)
	l := New(rdb)
	limit := config.RateLimit{RequestsPerSecond: 2, Burst: 2}
	start := time.Unix(1_700_000_000, 0)
	mini.SetTime(start)

	for range 2 {
		l.Allow(context.Background(), "acme", limit)
	}
	if res, _ := l.Allow(context.Background(), "acme", limit); res.Allowed || res.RetryAfter != 500*time.Millisecond {
		t.Fatalf("empty bucket: allowed=%v retry=%v, want rejected with 500ms retry at 2 tokens/s", res.Allowed, res.RetryAfter)
	}

	mini.SetTime(start.Add(750 * time.Millisecond)) // refills 1.5 tokens
	if res, _ := l.Allow(context.Background(), "acme", limit); !res.Allowed || res.Remaining != 0.5 {
		t.Fatalf("after 750ms: allowed=%v remaining=%v, want allowed with 0.5 left", res.Allowed, res.Remaining)
	}
	if res, _ := l.Allow(context.Background(), "acme", limit); res.Allowed {
		t.Fatal("only half a token left; request should be rejected")
	}

	mini.SetTime(start.Add(time.Hour)) // refill caps at capacity
	for i := range 3 {
		res, _ := l.Allow(context.Background(), "acme", limit)
		if want := i < 2; res.Allowed != want {
			t.Fatalf("after idling, request %d allowed=%v, want %v (bucket caps at burst=2)", i, res.Allowed, want)
		}
	}
}

func TestConcurrentCallsNeverExceedBurst(t *testing.T) {
	rdb, _ := redistest.New(t)
	l, tenant := New(rdb), uniqueTenant(t)
	limit := config.RateLimit{RequestsPerSecond: 0.001, Burst: 40} // effectively no refill during the test

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			for range 4 {
				res, err := l.Allow(context.Background(), tenant, limit)
				if err != nil {
					t.Error(err)
					return
				}
				if res.Allowed {
					allowed.Add(1)
				}
			}
		})
	}
	wg.Wait()
	if got := allowed.Load(); got != 40 {
		t.Fatalf("allowed %d of 200 concurrent requests, want exactly the burst of 40", got)
	}
}

func TestTenantsHaveSeparateBuckets(t *testing.T) {
	rdb, _ := redistest.New(t)
	l := New(rdb)
	limit := config.RateLimit{RequestsPerSecond: 0.001, Burst: 1}
	a, b := uniqueTenant(t)+"-a", uniqueTenant(t)+"-b"
	l.Allow(context.Background(), a, limit)
	if res, _ := l.Allow(context.Background(), b, limit); !res.Allowed {
		t.Fatal("tenant b must not be limited by tenant a's usage")
	}
}
