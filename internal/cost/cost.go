// Package cost prices requests and keeps a per-tenant spend ledger in Redis.
//
// Money is never a float here. Config prices (USD per million tokens) are converted once to integer micro-dollars
// per million tokens, which is the same number as picodollars per token, so a request's cost is an exact integer
// product; it is rounded to whole micro-dollars ($0.000001) only when added to the ledger with HINCRBY.
package cost

import (
	"context"
	"math"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/redis/go-redis/v9"

	"github.com/Bhavd33p/llm-gateway/internal/config"
	"github.com/Bhavd33p/llm-gateway/internal/metrics"
	"github.com/Bhavd33p/llm-gateway/internal/provider"
)

type price struct {
	inputPicoPerToken  int64
	outputPicoPerToken int64
}

type Pricer struct {
	prices map[string]price
}

func NewPricer(table map[string]config.Price) *Pricer {
	p := &Pricer{prices: make(map[string]price, len(table))}
	for model, pr := range table {
		p.prices[model] = price{
			inputPicoPerToken:  int64(math.Round(pr.InputPerMillion * 1e6)),
			outputPicoPerToken: int64(math.Round(pr.OutputPerMillion * 1e6)),
		}
	}
	return p
}

// MicroUSD returns what usage costs on model in micro-dollars (rounded half up), and false if the model has no price.
func (p *Pricer) MicroUSD(model string, u provider.Usage) (int64, bool) {
	pr, ok := p.prices[model]
	if !ok {
		return 0, false
	}
	pico := int64(u.PromptTokens)*pr.inputPicoPerToken + int64(u.CompletionTokens)*pr.outputPicoPerToken
	return (pico + 500_000) / 1_000_000, true
}

// EstimateUsage approximates token counts at about four characters per token (OpenAI's rule of thumb for English
// text) plus a few tokens of per-message overhead. It is used only when a stream ends without a usage report, and
// such requests are counted in the ledger's estimated_requests field and the llm_gateway_estimated_usage_total metric.
func EstimateUsage(messages []provider.Message, completion string) provider.Usage {
	prompt := 0
	for _, m := range messages {
		prompt += estimateTokens(m.Content) + 4
	}
	u := provider.Usage{PromptTokens: prompt, CompletionTokens: estimateTokens(completion)}
	u.TotalTokens = u.PromptTokens + u.CompletionTokens
	return u
}

func estimateTokens(s string) int {
	return (utf8.RuneCountInString(s) + 3) / 4
}

type Ledger struct {
	rdb     redis.Cmdable
	pricer  *Pricer
	metrics *metrics.Metrics
	now     func() time.Time
}

func NewLedger(rdb redis.Cmdable, pricer *Pricer, m *metrics.Metrics) *Ledger {
	return &Ledger{rdb: rdb, pricer: pricer, metrics: m, now: time.Now}
}

// Key is the tenant's ledger hash for one calendar month (UTC), e.g. ledger:{acme}:2026-11.
func Key(tenant string, at time.Time) string {
	return "ledger:{" + tenant + "}:" + at.UTC().Format("2006-01")
}

type Record struct {
	Tenant    string
	Model     string
	Upstream  string
	Usage     provider.Usage
	Estimated bool // token counts came from EstimateUsage, not the provider
	CacheHit  bool // served from cache: nothing spent, the original cost is recorded as saved
}

// Record adds one request to the ledger and returns its cost in micro-dollars. HINCRBY is atomic per field, so
// concurrent gateway replicas can all write to the same ledger without locks.
func (l *Ledger) Record(ctx context.Context, rec Record) (int64, error) {
	key := Key(rec.Tenant, l.now())
	cost, priced := l.pricer.MicroUSD(rec.Model, rec.Usage)
	if !priced {
		l.metrics.UnpricedModels.WithLabelValues(rec.Model).Inc()
	}

	pipe := l.rdb.Pipeline()
	if rec.CacheHit {
		pipe.HIncrBy(ctx, key, "cache_hits", 1)
		pipe.HIncrBy(ctx, key, "saved_micro_usd", cost)
		_, err := pipe.Exec(ctx)
		return 0, err
	}

	pipe.HIncrBy(ctx, key, "requests", 1)
	pipe.HIncrBy(ctx, key, "prompt_tokens", int64(rec.Usage.PromptTokens))
	pipe.HIncrBy(ctx, key, "completion_tokens", int64(rec.Usage.CompletionTokens))
	pipe.HIncrBy(ctx, key, "spend_micro_usd", cost)
	pipe.HIncrBy(ctx, key, "spend_micro_usd:"+rec.Model, cost)
	if rec.Estimated {
		pipe.HIncrBy(ctx, key, "estimated_requests", 1)
		l.metrics.EstimatedUsage.WithLabelValues(rec.Upstream).Inc()
	}
	l.metrics.TenantSpendUSD.WithLabelValues(rec.Tenant, rec.Model).Add(float64(cost) / 1e6)
	l.metrics.Tokens.WithLabelValues(rec.Tenant, rec.Model, "prompt").Add(float64(rec.Usage.PromptTokens))
	l.metrics.Tokens.WithLabelValues(rec.Tenant, rec.Model, "completion").Add(float64(rec.Usage.CompletionTokens))
	_, err := pipe.Exec(ctx)
	return cost, err
}

// Month returns the tenant's ledger fields for the current month.
func (l *Ledger) Month(ctx context.Context, tenant string) (map[string]int64, error) {
	raw, err := l.rdb.HGetAll(ctx, Key(tenant, l.now())).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(raw))
	for field, v := range raw {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, err
		}
		out[field] = n
	}
	return out, nil
}
