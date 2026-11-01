// Package config loads and validates the gateway's YAML configuration.
package config

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

type Config struct {
	Server    Server           `yaml:"server"`
	Redis     Redis            `yaml:"redis"`
	Routing   Routing          `yaml:"routing"`
	Upstreams []Upstream       `yaml:"upstreams"`
	Embedder  Embedder         `yaml:"embedder"`
	Cache     Cache            `yaml:"cache"`
	Pricing   map[string]Price `yaml:"pricing"`
	Tenants   []Tenant         `yaml:"tenants"`
}

type Server struct {
	Addr            string        `yaml:"addr"`
	MaxBodyBytes    int64         `yaml:"max_body_bytes"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

// Redis holds the connection settings. The timeouts are deliberately tight: Redis only backs optional features
// (cache, rate limits, ledger), so a slow or dead Redis must cost a request milliseconds, not seconds.
type Redis struct {
	Addr         string        `yaml:"addr"`
	PasswordEnv  string        `yaml:"password_env"`
	DB           int           `yaml:"db"`
	PoolSize     int           `yaml:"pool_size"`
	DialTimeout  time.Duration `yaml:"dial_timeout"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	PoolTimeout  time.Duration `yaml:"pool_timeout"`
	// MaxRetries is how many times a failed command is retried; 0 means no retries.
	MaxRetries              int           `yaml:"max_retries"`
	BreakerFailureThreshold int           `yaml:"breaker_failure_threshold"`
	BreakerCooldown         time.Duration `yaml:"breaker_cooldown"`

	Password string `yaml:"-"`
}

type Routing struct {
	MaxAttempts             int           `yaml:"max_attempts"`
	BackoffBase             time.Duration `yaml:"backoff_base"`
	BackoffMax              time.Duration `yaml:"backoff_max"`
	BreakerFailureThreshold int           `yaml:"breaker_failure_threshold"`
	BreakerCooldown         time.Duration `yaml:"breaker_cooldown"`
}

type Upstream struct {
	Name      string `yaml:"name"`
	Type      string `yaml:"type"`
	BaseURL   string `yaml:"base_url"`
	APIKeyEnv string `yaml:"api_key_env"`
	Weight    int    `yaml:"weight"`
	// Timeout bounds a whole non-streaming request; FirstTokenTimeout bounds a stream until its first chunk.
	Timeout           time.Duration     `yaml:"timeout"`
	FirstTokenTimeout time.Duration     `yaml:"first_token_timeout"`
	MaxIdleConns      int               `yaml:"max_idle_conns"`
	MaxConns          int               `yaml:"max_conns"`
	ModelMap          map[string]string `yaml:"model_map"`

	APIKey string `yaml:"-"`
}

type Embedder struct {
	Type      string        `yaml:"type"`
	Dim       int           `yaml:"dim"`
	BaseURL   string        `yaml:"base_url"`
	APIKeyEnv string        `yaml:"api_key_env"`
	Model     string        `yaml:"model"`
	Timeout   time.Duration `yaml:"timeout"`

	APIKey string `yaml:"-"`
}

// Cache holds semantic-cache settings. Tenants may override any field; zero values inherit the global block.
type Cache struct {
	Enabled    *bool         `yaml:"enabled"`
	Threshold  float64       `yaml:"threshold"`
	MaxEntries int           `yaml:"max_entries"`
	TTL        time.Duration `yaml:"ttl"`
}

func (c Cache) IsEnabled() bool { return c.Enabled != nil && *c.Enabled }

// Price is USD per one million tokens. Floats stop here: the cost package converts them to integers once at startup.
type Price struct {
	InputPerMillion  float64 `yaml:"input_per_million"`
	OutputPerMillion float64 `yaml:"output_per_million"`
}

type Tenant struct {
	Name string `yaml:"name"`
	// APIKeySHA256 lists hex SHA-256 digests of the tenant's keys, so plaintext keys never live in config.
	// More than one entry allows rotating a key without downtime.
	APIKeySHA256 []string  `yaml:"api_key_sha256"`
	RateLimit    RateLimit `yaml:"rate_limit"`
	Cache        Cache     `yaml:"cache"`
}

type RateLimit struct {
	RequestsPerSecond float64 `yaml:"requests_per_second"`
	Burst             int     `yaml:"burst"`
}

const (
	UpstreamOpenAI    = "openai"
	UpstreamAnthropic = "anthropic"
	EmbedderHash      = "hash"
	EmbedderOpenAI    = "openai"
)

// Tenant names are embedded in Redis keys inside {hash tags}, so braces and spaces are not allowed.
var tenantNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// Load reads the YAML file at path, applies defaults, resolves secrets from the environment and validates the result.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(raw)
}

func Parse(raw []byte) (*Config, error) {
	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true) // a typo such as "max_entires" should fail loudly, not silently fall back to a default
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.applyDefaults()
	if err := cfg.resolveSecrets(); err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	setDefault(&c.Server.Addr, ":8080")
	setDefault(&c.Server.MaxBodyBytes, 1<<20)
	setDefault(&c.Server.ShutdownTimeout, 30*time.Second)

	setDefault(&c.Redis.Addr, "localhost:6379")
	setDefault(&c.Redis.DialTimeout, 50*time.Millisecond)
	setDefault(&c.Redis.ReadTimeout, 50*time.Millisecond)
	setDefault(&c.Redis.WriteTimeout, 50*time.Millisecond)
	setDefault(&c.Redis.PoolTimeout, 50*time.Millisecond)
	setDefault(&c.Redis.BreakerFailureThreshold, 5)
	setDefault(&c.Redis.BreakerCooldown, 5*time.Second)

	setDefault(&c.Routing.MaxAttempts, 3)
	setDefault(&c.Routing.BackoffBase, 50*time.Millisecond)
	setDefault(&c.Routing.BackoffMax, time.Second)
	setDefault(&c.Routing.BreakerFailureThreshold, 5)
	setDefault(&c.Routing.BreakerCooldown, 10*time.Second)

	for i := range c.Upstreams {
		u := &c.Upstreams[i]
		setDefault(&u.Weight, 1)
		setDefault(&u.Timeout, 60*time.Second)
		setDefault(&u.FirstTokenTimeout, 15*time.Second)
		setDefault(&u.MaxIdleConns, 256)
	}

	setDefault(&c.Embedder.Type, EmbedderHash)
	setDefault(&c.Embedder.Dim, 256)
	setDefault(&c.Embedder.Model, "text-embedding-3-small")
	setDefault(&c.Embedder.Timeout, 5*time.Second)

	if c.Cache.Enabled == nil {
		enabled := true
		c.Cache.Enabled = &enabled
	}
	setDefault(&c.Cache.Threshold, 0.95)
	setDefault(&c.Cache.MaxEntries, 1000)
	setDefault(&c.Cache.TTL, 24*time.Hour)

	for i := range c.Tenants {
		for j, h := range c.Tenants[i].APIKeySHA256 {
			c.Tenants[i].APIKeySHA256[j] = strings.ToLower(strings.TrimSpace(h))
		}
		t := &c.Tenants[i].Cache
		if t.Enabled == nil {
			t.Enabled = c.Cache.Enabled
		}
		setDefault(&t.Threshold, c.Cache.Threshold)
		setDefault(&t.MaxEntries, c.Cache.MaxEntries)
		setDefault(&t.TTL, c.Cache.TTL)
	}
}

func setDefault[T comparable](field *T, value T) {
	var zero T
	if *field == zero {
		*field = value
	}
}

func (c *Config) resolveSecrets() error {
	var err error
	if c.Redis.Password, err = lookupEnv(c.Redis.PasswordEnv); err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	for i := range c.Upstreams {
		u := &c.Upstreams[i]
		if u.APIKey, err = lookupEnv(u.APIKeyEnv); err != nil {
			return fmt.Errorf("upstream %q: %w", u.Name, err)
		}
	}
	if c.Embedder.Type == EmbedderOpenAI {
		if c.Embedder.APIKey, err = lookupEnv(c.Embedder.APIKeyEnv); err != nil {
			return fmt.Errorf("embedder: %w", err)
		}
	}
	return nil
}

// lookupEnv returns "" when no variable is named, and an error when a named variable is unset,
// so a missing provider key fails at startup instead of on the first request.
func lookupEnv(name string) (string, error) {
	if name == "" {
		return "", nil
	}
	v, ok := os.LookupEnv(name)
	if !ok || v == "" {
		return "", fmt.Errorf("environment variable %s is not set", name)
	}
	return v, nil
}

func (c *Config) validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if c.Redis.MaxRetries < 0 {
		add("redis.max_retries must not be negative")
	}
	if c.Routing.MaxAttempts < 1 {
		add("routing.max_attempts must be at least 1")
	}
	if c.Routing.BackoffMax < c.Routing.BackoffBase {
		add("routing.backoff_max must be >= routing.backoff_base")
	}

	if len(c.Upstreams) == 0 {
		add("at least one upstream is required")
	}
	upstreamNames := map[string]bool{}
	for _, u := range c.Upstreams {
		if u.Name == "" {
			add("upstream name is required")
		} else if upstreamNames[u.Name] {
			add("duplicate upstream name %q", u.Name)
		}
		upstreamNames[u.Name] = true
		if u.Type != UpstreamOpenAI && u.Type != UpstreamAnthropic {
			add("upstream %q: type must be %q or %q", u.Name, UpstreamOpenAI, UpstreamAnthropic)
		}
		if parsed, err := url.Parse(u.BaseURL); err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			add("upstream %q: base_url must be an absolute http(s) URL", u.Name)
		}
		if u.Weight < 1 {
			add("upstream %q: weight must be at least 1", u.Name)
		}
	}

	switch c.Embedder.Type {
	case EmbedderHash:
		if c.Embedder.Dim < 8 {
			add("embedder.dim must be at least 8")
		}
	case EmbedderOpenAI:
		if c.Embedder.BaseURL == "" {
			add("embedder.base_url is required for the openai embedder")
		}
	default:
		add("embedder.type must be %q or %q", EmbedderHash, EmbedderOpenAI)
	}

	for model, p := range c.Pricing {
		if p.InputPerMillion < 0 || p.OutputPerMillion < 0 {
			add("pricing %q: prices must not be negative", model)
		}
	}

	if len(c.Tenants) == 0 {
		add("at least one tenant is required")
	}
	tenantNames := map[string]bool{}
	keyOwners := map[string]string{}
	for _, t := range c.Tenants {
		if !tenantNamePattern.MatchString(t.Name) {
			add("tenant name %q must match %s", t.Name, tenantNamePattern)
		} else if tenantNames[t.Name] {
			add("duplicate tenant name %q", t.Name)
		}
		tenantNames[t.Name] = true
		if len(t.APIKeySHA256) == 0 {
			add("tenant %q: at least one api_key_sha256 is required", t.Name)
		}
		for _, h := range t.APIKeySHA256 {
			if b, err := hex.DecodeString(h); err != nil || len(b) != 32 {
				add("tenant %q: api_key_sha256 %q is not a hex SHA-256 digest", t.Name, h)
			} else if owner, dup := keyOwners[h]; dup {
				add("tenant %q: api key hash already used by tenant %q", t.Name, owner)
			}
			keyOwners[h] = t.Name
		}
		if t.RateLimit.RequestsPerSecond <= 0 || t.RateLimit.Burst < 1 {
			add("tenant %q: rate_limit.requests_per_second must be > 0 and rate_limit.burst >= 1", t.Name)
		}
		if t.Cache.Threshold <= 0 || t.Cache.Threshold > 1 {
			add("tenant %q: cache.threshold must be in (0, 1]", t.Name)
		}
		if t.Cache.MaxEntries < 1 || t.Cache.TTL <= 0 {
			add("tenant %q: cache.max_entries and cache.ttl must be positive", t.Name)
		}
	}
	return errors.Join(errs...)
}
