// Package redistest gives tests a Redis client: a real server when LLMGW_TEST_REDIS_ADDR is set (CI runs a Redis
// service container), otherwise an in-process miniredis.
package redistest

import (
	"context"
	"os"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// New returns a client and, when running against miniredis, the server so the test can control its clock.
// The miniredis return value is nil against a real Redis. Tests isolate themselves with unique key names.
func New(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	if addr := os.Getenv("LLMGW_TEST_REDIS_ADDR"); addr != "" {
		client := redis.NewClient(&redis.Options{Addr: addr})
		if err := client.Ping(context.Background()).Err(); err != nil {
			t.Fatalf("redis at %s: %v", addr, err)
		}
		t.Cleanup(func() { client.Close() })
		return client, nil
	}
	return NewMini(t)
}

// NewMini always uses miniredis, for tests that need to move time forward.
func NewMini(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mini := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { client.Close() })
	return client, mini
}
