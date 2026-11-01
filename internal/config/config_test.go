package config

import (
	"strings"
	"testing"
	"time"
)

const minimalConfig = `
upstreams:
  - name: mock-a
    type: openai
    base_url: http://localhost:9001/v1
tenants:
  - name: acme
    api_key_sha256: ["8649CDEBEE753898FD50E01408A9E438AA5090249BC3E51B65F33DCE973B0873"]
    rate_limit: {requests_per_second: 5, burst: 10}
`

func TestParseAppliesDefaults(t *testing.T) {
	cfg, err := Parse([]byte(minimalConfig))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Server.Addr != ":8080" || cfg.Server.ShutdownTimeout != 30*time.Second {
		t.Errorf("server defaults not applied: %+v", cfg.Server)
	}
	if cfg.Routing.MaxAttempts != 3 || cfg.Routing.BreakerFailureThreshold != 5 {
		t.Errorf("routing defaults not applied: %+v", cfg.Routing)
	}
	if u := cfg.Upstreams[0]; u.Weight != 1 || u.MaxIdleConns != 256 {
		t.Errorf("upstream defaults not applied: %+v", u)
	}
	tenant := cfg.Tenants[0]
	if !tenant.Cache.IsEnabled() || tenant.Cache.Threshold != 0.95 || tenant.Cache.MaxEntries != 1000 || tenant.Cache.TTL != 24*time.Hour {
		t.Errorf("tenant cache should inherit global defaults: %+v", tenant.Cache)
	}
	if got := tenant.APIKeySHA256[0]; got != strings.ToLower(got) {
		t.Errorf("key hash should be lower-cased, got %s", got)
	}
}

func TestTenantCacheOverride(t *testing.T) {
	cfg, err := Parse([]byte(minimalConfig + `
cache:
  threshold: 0.9
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := cfg.Tenants[0].Cache.Threshold; got != 0.9 {
		t.Errorf("tenant should inherit global threshold 0.9, got %v", got)
	}

	disabled := strings.Replace(minimalConfig, "rate_limit:", "cache: {enabled: false, threshold: 0.99}\n    rate_limit:", 1)
	cfg, err = Parse([]byte(disabled))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if c := cfg.Tenants[0].Cache; c.IsEnabled() || c.Threshold != 0.99 {
		t.Errorf("tenant override not applied: %+v", c)
	}
}

func TestParseResolvesAPIKeyFromEnv(t *testing.T) {
	t.Setenv("TEST_UPSTREAM_KEY", "sk-test")
	cfg, err := Parse([]byte(strings.Replace(minimalConfig, "base_url:", "api_key_env: TEST_UPSTREAM_KEY\n    base_url:", 1)))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Upstreams[0].APIKey != "sk-test" {
		t.Errorf("APIKey = %q, want sk-test", cfg.Upstreams[0].APIKey)
	}
}

func TestParseRejectsInvalidConfig(t *testing.T) {
	cases := map[string]struct {
		yaml    string
		wantErr string
	}{
		"unknown field": {
			yaml:    minimalConfig + "\ncache: {max_entires: 5}\n",
			wantErr: "max_entires",
		},
		"missing env key": {
			yaml:    strings.Replace(minimalConfig, "base_url:", "api_key_env: SURELY_NOT_SET_LLMGW\n    base_url:", 1),
			wantErr: "SURELY_NOT_SET_LLMGW",
		},
		"bad upstream type": {
			yaml:    strings.Replace(minimalConfig, "type: openai", "type: gemini", 1),
			wantErr: "type must be",
		},
		"relative base url": {
			yaml:    strings.Replace(minimalConfig, "http://localhost:9001/v1", "/v1", 1),
			wantErr: "base_url",
		},
		"brace in tenant name": {
			yaml:    strings.Replace(minimalConfig, "name: acme", "name: ac{me}", 1),
			wantErr: "tenant name",
		},
		"bad key hash": {
			yaml:    strings.Replace(minimalConfig, "8649CDEBEE753898FD50E01408A9E438AA5090249BC3E51B65F33DCE973B0873", "plaintext-key", 1),
			wantErr: "not a hex SHA-256",
		},
		"missing rate limit": {
			yaml:    strings.Replace(minimalConfig, "rate_limit: {requests_per_second: 5, burst: 10}", "rate_limit: {}", 1),
			wantErr: "rate_limit",
		},
		"threshold above one": {
			yaml:    minimalConfig + "\ncache: {threshold: 1.5}\n",
			wantErr: "threshold",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.wantErr)
			}
		})
	}
}

func TestExampleConfigsAreValid(t *testing.T) {
	for _, path := range []string{"../../config.example.yaml", "../../deploy/config.compose.yaml"} {
		if _, err := Load(path); err != nil {
			t.Errorf("%s: %v", path, err)
		}
	}
}
