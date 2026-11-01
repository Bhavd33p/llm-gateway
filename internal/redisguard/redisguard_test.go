package redisguard

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"

	"github.com/Bhavd33p/llm-gateway/internal/breaker"
	"github.com/Bhavd33p/llm-gateway/internal/config"
	"github.com/Bhavd33p/llm-gateway/internal/metrics"
)

// deadAddr returns a local address with nothing listening, so connections are refused.
func deadAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func newClient(t *testing.T, addr string, cooldown time.Duration) (*redis.Client, *metrics.Metrics) {
	t.Helper()
	m := metrics.New(prometheus.NewRegistry())
	rdb := NewClient(config.Redis{
		Addr: addr, DialTimeout: 50 * time.Millisecond, ReadTimeout: 50 * time.Millisecond,
		WriteTimeout: 50 * time.Millisecond, PoolTimeout: 50 * time.Millisecond,
		BreakerFailureThreshold: 3, BreakerCooldown: cooldown,
	}, m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { rdb.Close() })
	return rdb, m
}

func TestDeadRedisFailsFastThenBreakerSkipsIt(t *testing.T) {
	rdb, m := newClient(t, deadAddr(t), time.Minute)
	ctx := context.Background()

	for i := range 3 {
		start := time.Now()
		err := rdb.Get(ctx, "k").Err()
		if !IsUnavailable(err) || errors.Is(err, ErrUnavailable) {
			t.Fatalf("call %d: err = %v, want a connection error", i, err)
		}
		if d := time.Since(start); d > 100*time.Millisecond {
			t.Fatalf("call %d took %v; a refused connection must fail fast (no dial or command retries)", i, d)
		}
	}
	if got := testutil.ToFloat64(m.RedisBreakerState); got != float64(breaker.Open) {
		t.Fatalf("breaker state = %v, want open after 3 consecutive failures", got)
	}

	start := time.Now()
	if err := rdb.Get(ctx, "k").Err(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable while the breaker is open", err)
	}
	pipe := rdb.Pipeline()
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); !errors.Is(err, ErrUnavailable) || !errors.Is(get.Err(), ErrUnavailable) {
		t.Fatalf("pipeline err = %v, cmd err = %v, want ErrUnavailable", err, get.Err())
	}
	if d := time.Since(start); d > 5*time.Millisecond {
		t.Fatalf("calls with an open breaker took %v; they should not touch the network", d)
	}
}

func TestBreakerClosesWhenRedisComesBack(t *testing.T) {
	mini := miniredis.RunT(t)
	rdb, m := newClient(t, mini.Addr(), 100*time.Millisecond)
	ctx := context.Background()
	if err := rdb.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatal(err)
	}

	mini.Close()
	for range 3 {
		rdb.Get(ctx, "k")
	}
	if got := testutil.ToFloat64(m.RedisBreakerState); got != float64(breaker.Open) {
		t.Fatalf("breaker state = %v, want open while Redis is down", got)
	}

	if err := mini.Restart(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for rdb.Ping(ctx).Err() != nil && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := testutil.ToFloat64(m.RedisBreakerState); got != float64(breaker.Closed) {
		t.Fatalf("breaker state = %v, want closed after a successful probe", got)
	}
}

func TestRedisAnswersDoNotTripTheBreaker(t *testing.T) {
	mini := miniredis.RunT(t)
	rdb, m := newClient(t, mini.Addr(), time.Minute)
	ctx := context.Background()
	rdb.Set(ctx, "str", "v", 0)
	for range 5 {
		if err := rdb.Get(ctx, "missing").Err(); !errors.Is(err, redis.Nil) || IsUnavailable(err) {
			t.Fatalf("missing key: err = %v, want redis.Nil and not unavailable", err)
		}
		if err := rdb.LPush(ctx, "str", "x").Err(); err == nil || IsUnavailable(err) {
			t.Fatalf("WRONGTYPE: err = %v, want a server error that is not an outage", err)
		}
	}
	if got := testutil.ToFloat64(m.RedisBreakerState); got != float64(breaker.Closed) {
		t.Fatalf("breaker state = %v; replies from a live Redis must not open it", got)
	}
}
