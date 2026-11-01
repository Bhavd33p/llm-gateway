package cost

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Bhavd33p/llm-gateway/internal/config"
	"github.com/Bhavd33p/llm-gateway/internal/metrics"
	"github.com/Bhavd33p/llm-gateway/internal/provider"
	"github.com/Bhavd33p/llm-gateway/internal/redistest"
)

var table = map[string]config.Price{
	"gpt-4o-mini": {InputPerMillion: 0.15, OutputPerMillion: 0.60},
	"gpt-4o":      {InputPerMillion: 2.50, OutputPerMillion: 10.00},
}

func TestPricerUsesIntegerMicroDollars(t *testing.T) {
	p := NewPricer(table)
	cases := []struct {
		model            string
		prompt, complete int
		want             int64
	}{
		{"gpt-4o-mini", 1000, 500, 450},              // $0.00015 + $0.0003 = $0.00045
		{"gpt-4o", 1_000_000, 1_000_000, 12_500_000}, // $2.50 + $10.00
		{"gpt-4o-mini", 3, 0, 0},                     // 0.45 micro-dollars rounds down
		{"gpt-4o-mini", 4, 0, 1},                     // 0.6 micro-dollars rounds up
	}
	for _, tc := range cases {
		got, ok := p.MicroUSD(tc.model, provider.Usage{PromptTokens: tc.prompt, CompletionTokens: tc.complete})
		if !ok || got != tc.want {
			t.Errorf("%s %d/%d tokens = %d µUSD (priced=%v), want %d", tc.model, tc.prompt, tc.complete, got, ok, tc.want)
		}
	}
	if _, ok := p.MicroUSD("unknown", provider.Usage{PromptTokens: 10}); ok {
		t.Error("an unknown model must report priced=false")
	}
}

func TestEstimateUsage(t *testing.T) {
	u := EstimateUsage([]provider.Message{{Role: "user", Content: "12345678"}}, "abcd")
	if u.PromptTokens != 2+4 || u.CompletionTokens != 1 || u.TotalTokens != 7 {
		t.Errorf("estimate = %+v", u)
	}
}

func TestLedgerAccumulates(t *testing.T) {
	rdb, _ := redistest.New(t)
	m := metrics.New(prometheus.NewRegistry())
	l := NewLedger(rdb, NewPricer(table), m)
	l.now = func() time.Time { return time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC) }
	tenant := fmt.Sprintf("t%d", time.Now().UnixNano())
	ctx := context.Background()

	for range 2 {
		cost, err := l.Record(ctx, Record{Tenant: tenant, Model: "gpt-4o-mini", Upstream: "a", Usage: provider.Usage{PromptTokens: 1000, CompletionTokens: 500}})
		if err != nil || cost != 450 {
			t.Fatalf("Record = %d, %v", cost, err)
		}
	}
	l.Record(ctx, Record{Tenant: tenant, Model: "gpt-4o-mini", Upstream: "a", Usage: provider.Usage{PromptTokens: 10, CompletionTokens: 10}, Estimated: true})
	l.Record(ctx, Record{Tenant: tenant, Model: "gpt-4o-mini", Usage: provider.Usage{PromptTokens: 1000, CompletionTokens: 500}, CacheHit: true})

	got, err := l.Month(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{
		"requests": 3, "prompt_tokens": 2010, "completion_tokens": 1010,
		"spend_micro_usd": 908, "spend_micro_usd:gpt-4o-mini": 908, "estimated_requests": 1,
		"cache_hits": 1, "saved_micro_usd": 450,
	}
	for field, v := range want {
		if got[field] != v {
			t.Errorf("%s = %d, want %d (ledger %v)", field, got[field], v, got)
		}
	}
	if key := Key(tenant, l.now()); key != "ledger:{"+tenant+"}:2026-11" {
		t.Errorf("key = %s", key)
	}
	if spend := testutil.ToFloat64(m.TenantSpendUSD.WithLabelValues(tenant, "gpt-4o-mini")); spend < 0.000907 || spend > 0.000909 {
		t.Errorf("spend metric = %v, want about 0.000908", spend)
	}
}

func TestLedgerCountsUnpricedModels(t *testing.T) {
	rdb, _ := redistest.New(t)
	m := metrics.New(prometheus.NewRegistry())
	l := NewLedger(rdb, NewPricer(table), m)
	cost, err := l.Record(context.Background(), Record{Tenant: fmt.Sprintf("t%d", time.Now().UnixNano()), Model: "mystery", Usage: provider.Usage{PromptTokens: 5}})
	if err != nil || cost != 0 {
		t.Fatalf("Record = %d, %v", cost, err)
	}
	if got := testutil.ToFloat64(m.UnpricedModels.WithLabelValues("mystery")); got != 1 {
		t.Errorf("unpriced metric = %v, want 1", got)
	}
}
