// Package cache is a per-tenant semantic cache in Redis: requests are embedded, and a stored response is served
// when a new request's embedding is close enough (cosine similarity at or above a threshold) to a cached one.
//
// Key layout. The {tenant} hash tag puts all of a tenant's keys in one Redis Cluster slot, so the MULTI/EXEC
// that writes an entry together with its index entry works on a cluster too:
//
//	sc:{tenant}:e:<id>  HASH  emb (little-endian float32 blob), resp (JSON), model, params, created_at (unix ms); has a TTL
//	sc:{tenant}:lru     ZSET  member <id>, score = last access time (unix ms); the LRU index
//
// Lookup is a linear scan: fetch every entry's embedding in one pipelined round trip and compare in Go. That is
// fine for the small per-tenant caches this targets (a thousand entries of 256 dims is about 1 MB per lookup);
// past that the next step is a vector index such as Redis Query Engine's HNSW, which answers top-k in Redis.
package cache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Bhavd33p/llm-gateway/internal/config"
	"github.com/Bhavd33p/llm-gateway/internal/metrics"
	"github.com/Bhavd33p/llm-gateway/internal/provider"
)

// Entry is the cached answer.
type Entry struct {
	Content      string `json:"content"`
	FinishReason string `json:"finish_reason"`
	// Usage and ServedModel describe the original upstream call, so a hit can record what it saved at that price.
	Usage       provider.Usage `json:"usage"`
	ServedModel string         `json:"served_model"`
}

type Hit struct {
	ID         string
	Entry      Entry
	Similarity float64
}

type Cache struct {
	rdb      redis.Cmdable
	embedder Embedder
	metrics  *metrics.Metrics
	now      func() time.Time
}

func New(rdb redis.Cmdable, embedder Embedder, m *metrics.Metrics) *Cache {
	return &Cache{rdb: rdb, embedder: embedder, metrics: m, now: time.Now}
}

func entryKey(tenant, id string) string { return "sc:{" + tenant + "}:e:" + id }
func lruKey(tenant string) string       { return "sc:{" + tenant + "}:lru" }

// PromptText is the canonical form of a request that gets embedded: the model and every message, lower-cased
// with whitespace collapsed, so trivially different spellings of one request embed identically.
func PromptText(model string, messages []provider.Message) string {
	var b strings.Builder
	b.WriteString("model: ")
	b.WriteString(model)
	for _, m := range messages {
		b.WriteString("\n")
		b.WriteString(m.Role)
		b.WriteString(": ")
		b.WriteString(strings.Join(strings.Fields(strings.ToLower(m.Content)), " "))
	}
	return b.String()
}

// ParamsKey fingerprints the request parameters that change what an answer looks like. Entries only match requests
// with the same fingerprint, so an answer generated with max_tokens=500 is never served to a request for 5 tokens,
// and a temperature-0 answer is not reused for a temperature-1 request.
func ParamsKey(req *provider.ChatRequest) string {
	optInt := func(p *int) string {
		if p == nil {
			return "-"
		}
		return strconv.Itoa(*p)
	}
	optFloat := func(p *float64) string {
		if p == nil {
			return "-"
		}
		return strconv.FormatFloat(*p, 'g', -1, 64)
	}
	return fmt.Sprintf("max_tokens=%s;temperature=%s;top_p=%s;stop=%q", optInt(req.MaxTokens), optFloat(req.Temperature), optFloat(req.TopP), []string(req.Stop))
}

// Embed embeds a request once; the vector is reused for the lookup and, on a miss, for storing the answer.
func (c *Cache) Embed(ctx context.Context, model string, messages []provider.Message) ([]float32, error) {
	return c.embedder.Embed(ctx, PromptText(model, messages))
}

// Lookup returns the most similar cached entry with the same model and parameters (see ParamsKey) if its similarity
// reaches the tenant's threshold, or nil on a miss. Model and parameters are compared exactly, because similar
// prompts to different models, or with different limits, must not share answers.
func (c *Cache) Lookup(ctx context.Context, tenant, model, params string, vec []float32, settings config.Cache) (hit *Hit, err error) {
	defer func() {
		if hit != nil {
			c.metrics.CacheHits.WithLabelValues(tenant).Inc()
		} else {
			c.metrics.CacheMisses.WithLabelValues(tenant).Inc()
		}
	}()

	ids, err := c.rdb.ZRange(ctx, lruKey(tenant), 0, -1).Result()
	if err != nil || len(ids) == 0 {
		return nil, err
	}

	// One pipelined round trip fetches every candidate's embedding, model and parameters.
	pipe := c.rdb.Pipeline()
	cmds := make([]*redis.SliceCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HMGet(ctx, entryKey(tenant, id), "emb", "model", "params")
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}

	bestID, bestSim := "", -1.0
	var expired []string
	for i, cmd := range cmds {
		fields := cmd.Val()
		blob, _ := fields[0].(string)
		entryModel, _ := fields[1].(string)
		entryParams, _ := fields[2].(string)
		if blob == "" {
			expired = append(expired, ids[i]) // the entry's TTL ran out but its index member remains
			continue
		}
		if entryModel != model || entryParams != params {
			continue
		}
		candidate, err := decodeVector([]byte(blob))
		if err != nil {
			continue
		}
		if sim := Cosine(vec, candidate); sim > bestSim {
			bestID, bestSim = ids[i], sim
		}
	}
	if len(expired) > 0 {
		c.rdb.ZRem(ctx, lruKey(tenant), toAny(expired)...) // best effort; a later lookup retries
	}
	if bestID == "" {
		return nil, nil
	}
	c.metrics.CacheSimilarity.Observe(bestSim)
	if bestSim < settings.Threshold {
		return nil, nil
	}

	// Fetch the answer and bump its LRU score in one round trip. ZADD XX only updates an existing member, so a
	// concurrently evicted entry is not resurrected in the index.
	pipe = c.rdb.Pipeline()
	resp := pipe.HGet(ctx, entryKey(tenant, bestID), "resp")
	pipe.ZAddXX(ctx, lruKey(tenant), redis.Z{Score: float64(c.now().UnixMilli()), Member: bestID})
	if _, err := pipe.Exec(ctx); err == redis.Nil {
		return nil, nil // evicted between the two round trips
	} else if err != nil {
		return nil, err
	}
	var entry Entry
	if err := json.Unmarshal([]byte(resp.Val()), &entry); err != nil {
		return nil, fmt.Errorf("decode cached entry: %w", err)
	}
	return &Hit{ID: bestID, Entry: entry, Similarity: bestSim}, nil
}

// Store saves an answer under a new id and evicts the least-recently-used entries beyond max_entries.
func (c *Cache) Store(ctx context.Context, tenant, model, params string, vec []float32, entry Entry, settings config.Cache) error {
	payload, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	id, err := newID()
	if err != nil {
		return err
	}
	now := c.now().UnixMilli()

	// MULTI/EXEC so the entry and its index member appear together.
	pipe := c.rdb.TxPipeline()
	pipe.HSet(ctx, entryKey(tenant, id), "emb", encodeVector(vec), "resp", payload, "model", model, "params", params, "created_at", now)
	pipe.PExpire(ctx, entryKey(tenant, id), settings.TTL)
	pipe.ZAdd(ctx, lruKey(tenant), redis.Z{Score: float64(now), Member: id})
	pipe.PExpire(ctx, lruKey(tenant), settings.TTL) // an idle tenant's index expires along with its entries
	size := pipe.ZCard(ctx, lruKey(tenant))
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	// Concurrent stores can each see the overflow and pop, evicting a few extra entries; that errs on the safe side.
	if over := size.Val() - int64(settings.MaxEntries); over > 0 {
		victims, err := c.rdb.ZPopMin(ctx, lruKey(tenant), over).Result()
		if err != nil {
			return err
		}
		keys := make([]string, len(victims))
		for i, v := range victims {
			keys[i] = entryKey(tenant, v.Member.(string))
		}
		if err := c.rdb.Del(ctx, keys...).Err(); err != nil {
			return err
		}
		c.metrics.CacheEvictions.WithLabelValues(tenant).Add(float64(len(victims)))
	}
	return nil
}

func newID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func toAny(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}
