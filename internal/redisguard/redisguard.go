// Package redisguard builds the gateway's Redis client so that a Redis outage costs a request milliseconds instead
// of seconds. Redis only backs optional features here (semantic cache, rate limits, cost ledger), so when it is down
// the right move is to stop waiting for it:
//
//   - tight dial, read, write and pool timeouts, one dial attempt and no command retries;
//   - a circuit breaker around every command and pipeline: after consecutive failures, calls are refused locally
//     with ErrUnavailable for a cooldown, then one probe is let through to see whether Redis is back.
package redisguard

import (
	"context"
	"errors"
	"log/slog"

	"github.com/redis/go-redis/v9"

	"github.com/Bhavd33p/llm-gateway/internal/breaker"
	"github.com/Bhavd33p/llm-gateway/internal/config"
	"github.com/Bhavd33p/llm-gateway/internal/metrics"
)

// ErrUnavailable is returned without touching the network while the Redis breaker is open.
var ErrUnavailable = errors.New("redis unavailable: circuit breaker open")

func NewClient(cfg config.Redis, m *metrics.Metrics, logger *slog.Logger) *redis.Client {
	maxRetries := cfg.MaxRetries
	if maxRetries == 0 {
		maxRetries = -1 // go-redis reads 0 as "use the default of 3"; -1 means no retries
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:        cfg.Addr,
		Password:    cfg.Password,
		DB:          cfg.DB,
		PoolSize:    cfg.PoolSize, // 0 = go-redis default of 10 per CPU
		DialTimeout: cfg.DialTimeout,
		// go-redis otherwise makes 5 dial attempts 100ms apart inside every command; with its 3 command retries
		// that is what made each request wait about 6 seconds while Redis was down.
		DialerRetries: 1,
		ReadTimeout:   cfg.ReadTimeout,
		WriteTimeout:  cfg.WriteTimeout,
		PoolTimeout:   cfg.PoolTimeout,
		MaxRetries:    maxRetries,
	})
	gauge := m.RedisBreakerState
	gauge.Set(float64(breaker.Closed))
	b := breaker.New(cfg.BreakerFailureThreshold, cfg.BreakerCooldown, func(s breaker.State) {
		gauge.Set(float64(s))
		// One log line per transition, not one per request, so an outage does not flood the logs.
		logger.Warn("redis circuit breaker changed state", "state", s.String())
	})
	rdb.AddHook(hook{breaker: b})
	return rdb
}

// IsUnavailable reports whether err means Redis could not be reached (breaker open, connection failure, timeout),
// as opposed to Redis answering with an error.
func IsUnavailable(err error) bool {
	return errors.Is(err, ErrUnavailable) || isOutage(err)
}

func isOutage(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, ErrUnavailable) {
		return false
	}
	var answered redis.Error // includes redis.Nil and server errors such as WRONGTYPE: the server is up
	return !errors.As(err, &answered)
}

// hook runs every command and pipeline (including MULTI/EXEC) through the breaker.
type hook struct {
	breaker *breaker.Breaker
}

// admittedKey marks a context whose call the breaker already let through. go-redis sends connection setup
// commands (HELLO, CLIENT SETINFO) through the same hooks with the caller's context; without this mark a half-open
// probe would be refused by its own breaker while opening its connection.
type admittedKey struct{}

func (h hook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h hook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if ctx.Value(admittedKey{}) != nil {
			return next(ctx, cmd)
		}
		if !h.breaker.Allow() {
			cmd.SetErr(ErrUnavailable)
			return ErrUnavailable
		}
		err := next(context.WithValue(ctx, admittedKey{}, true), cmd)
		h.record(err)
		return err
	}
}

func (h hook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if ctx.Value(admittedKey{}) != nil {
			return next(ctx, cmds)
		}
		if !h.breaker.Allow() {
			for _, cmd := range cmds {
				cmd.SetErr(ErrUnavailable)
			}
			return ErrUnavailable
		}
		err := next(context.WithValue(ctx, admittedKey{}, true), cmds)
		h.record(err)
		return err
	}
}

func (h hook) record(err error) {
	switch {
	case errors.Is(err, context.Canceled):
		// The caller gave up; that says nothing about Redis.
	case isOutage(err):
		h.breaker.Failure()
	default:
		h.breaker.Success()
	}
}
