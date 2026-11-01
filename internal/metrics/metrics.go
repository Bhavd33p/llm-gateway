// Package metrics defines every Prometheus series the gateway exports.
//
// Percentiles and ratios are computed in PromQL rather than in the process:
//
//	cache hit rate:        sum(rate(llm_gateway_cache_hits_total[5m])) / (sum(rate(llm_gateway_cache_hits_total[5m])) + sum(rate(llm_gateway_cache_misses_total[5m])))
//	p99 TTFT:              histogram_quantile(0.99, sum by (le) (rate(llm_gateway_ttft_seconds_bucket[5m])))
//	p95 upstream latency:  histogram_quantile(0.95, sum by (le, upstream) (rate(llm_gateway_upstream_request_duration_seconds_bucket[5m])))
//	spend per tenant:      sum by (tenant) (increase(llm_gateway_tenant_spend_usd_total[1h]))
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Buckets span cache hits (sub-millisecond) to slow upstream generations (tens of seconds).
var latencyBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}

type Metrics struct {
	Requests        *prometheus.CounterVec
	RequestDuration *prometheus.HistogramVec
	InFlight        prometheus.Gauge
	TTFT            *prometheus.HistogramVec

	CacheHits       *prometheus.CounterVec
	CacheMisses     *prometheus.CounterVec
	CacheSimilarity prometheus.Histogram
	CacheEvictions  *prometheus.CounterVec

	UpstreamDuration *prometheus.HistogramVec
	UpstreamErrors   *prometheus.CounterVec
	Failovers        *prometheus.CounterVec
	BreakerState     *prometheus.GaugeVec

	RateLimited *prometheus.CounterVec

	TenantSpendUSD    *prometheus.CounterVec
	Tokens            *prometheus.CounterVec
	EstimatedUsage    *prometheus.CounterVec
	UnpricedModels    *prometheus.CounterVec
	DependencyErrors  *prometheus.CounterVec
	RedisUnavailable  *prometheus.CounterVec
	RedisBreakerState prometheus.Gauge
}

// New registers all gateway metrics on reg. Tests pass a fresh prometheus.NewRegistry() so runs don't collide.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llm_gateway_requests_total",
			Help: "Chat completion requests handled, by tenant and HTTP status code.",
		}, []string{"tenant", "code"}),
		RequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "llm_gateway_request_duration_seconds",
			Help:    "End-to-end request duration inside the gateway, by cache result (hit, miss, bypass).",
			Buckets: latencyBuckets,
		}, []string{"cache"}),
		InFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "llm_gateway_inflight_requests",
			Help: "Chat completion requests currently being served.",
		}),
		TTFT: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "llm_gateway_ttft_seconds",
			Help:    "Time from request arrival to the first content byte flushed to the client. source is the upstream name or \"cache\".",
			Buckets: latencyBuckets,
		}, []string{"source"}),

		CacheHits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llm_gateway_cache_hits_total",
			Help: "Semantic cache lookups that found an entry above the similarity threshold.",
		}, []string{"tenant"}),
		CacheMisses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llm_gateway_cache_misses_total",
			Help: "Semantic cache lookups that found no entry above the similarity threshold.",
		}, []string{"tenant"}),
		CacheSimilarity: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "llm_gateway_cache_best_similarity",
			Help:    "Best cosine similarity seen per lookup; use it to tune the threshold.",
			Buckets: []float64{.5, .6, .7, .8, .85, .9, .92, .94, .95, .96, .97, .98, .99, 1},
		}),
		CacheEvictions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llm_gateway_cache_evictions_total",
			Help: "Cache entries evicted because a tenant exceeded max_entries.",
		}, []string{"tenant"}),

		UpstreamDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "llm_gateway_upstream_request_duration_seconds",
			Help:    "Duration of each upstream attempt, from sending the request to the last byte; outcome is ok, error or cancelled.",
			Buckets: latencyBuckets,
		}, []string{"upstream", "outcome"}),
		UpstreamErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llm_gateway_upstream_errors_total",
			Help: "Failed upstream attempts, by reason (connect, timeout, status_5xx, status_429, status_auth, status_4xx, stream, protocol).",
		}, []string{"upstream", "reason"}),
		Failovers: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llm_gateway_failovers_total",
			Help: "Retries that moved a request from one upstream to another.",
		}, []string{"from", "to"}),
		BreakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "llm_gateway_circuit_breaker_state",
			Help: "Circuit breaker state per upstream: 0 closed, 1 half-open, 2 open.",
		}, []string{"upstream"}),

		RateLimited: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llm_gateway_rate_limited_total",
			Help: "Requests rejected with 429 by the per-tenant token bucket.",
		}, []string{"tenant"}),

		TenantSpendUSD: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llm_gateway_tenant_spend_usd_total",
			Help: "Upstream spend in USD per tenant and model. The Redis ledger (integer micro-dollars) is the source of truth.",
		}, []string{"tenant", "model"}),
		Tokens: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llm_gateway_tokens_total",
			Help: "Tokens consumed per tenant and model; kind is prompt or completion.",
		}, []string{"tenant", "model", "kind"}),
		EstimatedUsage: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llm_gateway_estimated_usage_total",
			Help: "Requests whose token counts were estimated because the upstream reported no usage.",
		}, []string{"upstream"}),
		UnpricedModels: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llm_gateway_unpriced_requests_total",
			Help: "Requests for a model missing from the price table (recorded at zero cost).",
		}, []string{"model"}),
		DependencyErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llm_gateway_dependency_errors_total",
			Help: "Errors other than a Redis outage, per component (ratelimit, cache, embedder, ledger). Each fails open: the request is still served.",
		}, []string{"component"}),
		RedisUnavailable: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llm_gateway_redis_unavailable_total",
			Help: "Times a feature went without Redis because it was unreachable or its breaker was open: ratelimit (request allowed), cache_lookup (served as a miss), cache_store (answer not cached), ledger (cost not recorded).",
		}, []string{"component"}),
		RedisBreakerState: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "llm_gateway_redis_breaker_state",
			Help: "Redis circuit breaker state: 0 closed, 1 half-open, 2 open (calls skip Redis).",
		}),
	}

	reg.MustRegister(
		m.Requests, m.RequestDuration, m.InFlight, m.TTFT,
		m.CacheHits, m.CacheMisses, m.CacheSimilarity, m.CacheEvictions,
		m.UpstreamDuration, m.UpstreamErrors, m.Failovers, m.BreakerState,
		m.RateLimited,
		m.TenantSpendUSD, m.Tokens, m.EstimatedUsage, m.UnpricedModels, m.DependencyErrors, m.RedisUnavailable, m.RedisBreakerState,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}
