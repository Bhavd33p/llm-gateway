// Package ratelimit implements a per-tenant token bucket stored in Redis.
package ratelimit

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Bhavd33p/llm-gateway/internal/config"
)

//go:embed tokenbucket.lua
var tokenBucketSource string

// go-redis runs the script with EVALSHA and falls back to EVAL (which caches it) the first time Redis lacks it.
var tokenBucket = redis.NewScript(tokenBucketSource)

type Limiter struct {
	rdb redis.Scripter
}

func New(rdb redis.Scripter) *Limiter {
	return &Limiter{rdb: rdb}
}

type Result struct {
	Allowed    bool
	Remaining  float64
	RetryAfter time.Duration
}

// Key returns the bucket key. The {hash tag} pins every key of a tenant to one Redis Cluster slot.
func Key(tenant string) string {
	return "rl:{" + tenant + "}"
}

// Allow takes one token from the tenant's bucket. Each request costs one token; the bucket holds up to Burst
// tokens and refills at RequestsPerSecond.
func (l *Limiter) Allow(ctx context.Context, tenant string, limit config.RateLimit) (Result, error) {
	reply, err := tokenBucket.Run(ctx, l.rdb, []string{Key(tenant)}, limit.RequestsPerSecond, limit.Burst, 1).Slice()
	if err != nil {
		return Result{}, fmt.Errorf("token bucket: %w", err)
	}
	if len(reply) != 3 {
		return Result{}, fmt.Errorf("token bucket: unexpected reply %v", reply)
	}
	allowed, _ := reply[0].(int64)
	remainingText, _ := reply[1].(string)
	retryMillis, _ := reply[2].(int64)
	remaining, _ := strconv.ParseFloat(remainingText, 64)
	return Result{
		Allowed:    allowed == 1,
		Remaining:  remaining,
		RetryAfter: time.Duration(retryMillis) * time.Millisecond,
	}, nil
}
