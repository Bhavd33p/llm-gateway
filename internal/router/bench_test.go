package router

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Bhavd33p/llm-gateway/internal/metrics"
)

func BenchmarkPick(b *testing.B) {
	r := New([]Upstream{{Provider: ok("a"), Weight: 5}, {Provider: ok("b"), Weight: 1}, {Provider: ok("c"), Weight: 1}},
		Options{MaxAttempts: 1}, metrics.New(prometheus.NewRegistry()))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.candidates()
		}
	})
}
