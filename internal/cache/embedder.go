package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"net/http"
	"strings"
	"time"
	"unicode"

	"github.com/Bhavd33p/llm-gateway/internal/config"
)

// Embedder turns text into a vector whose cosine similarity reflects how alike two texts are.
type Embedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
}

// HashEmbedder is a deterministic, offline embedder based on the hashing trick: every word and every pair of
// adjacent words (n-grams of size 1 and 2) is hashed to one of dim buckets with a +1 or -1 sign, and the vector is
// normalised. Texts that share many n-grams point the same way. It captures wording, not meaning (synonyms do not
// match), which makes it right for tests and load tests and a stand-in for a real embedding model in production.
type HashEmbedder struct {
	dim int
}

func NewHashEmbedder(dim int) *HashEmbedder {
	return &HashEmbedder{dim: dim}
}

func (h *HashEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	vec := make([]float32, h.dim)
	words := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
	add := func(feature string) {
		hash := fnv.New64a()
		hash.Write([]byte(feature))
		sum := hash.Sum64()
		// The sign bit makes colliding features cancel out on average instead of always inflating similarity.
		sign := float32(1)
		if sum>>63 == 1 {
			sign = -1
		}
		vec[sum%uint64(h.dim)] += sign
	}
	for i, w := range words {
		add(w)
		if i > 0 {
			add(words[i-1] + " " + w)
		}
	}
	normalize(vec)
	return vec, nil
}

func normalize(v []float32) {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if sum == 0 {
		return
	}
	norm := float32(math.Sqrt(sum))
	for i := range v {
		v[i] /= norm
	}
}

// OpenAIEmbedder calls any OpenAI-compatible POST /embeddings endpoint (OpenAI, vLLM, Ollama, TEI behind a shim).
type OpenAIEmbedder struct {
	endpoint string
	apiKey   string
	model    string
	client   *http.Client
}

func NewOpenAIEmbedder(cfg config.Embedder) *OpenAIEmbedder {
	return &OpenAIEmbedder{
		endpoint: strings.TrimRight(cfg.BaseURL, "/") + "/embeddings",
		apiKey:   cfg.APIKey,
		model:    cfg.Model,
		client: &http.Client{
			Timeout:   cfg.Timeout,
			Transport: &http.Transport{MaxIdleConnsPerHost: 64, IdleConnTimeout: 90 * time.Second},
		},
	}
}

func (e *OpenAIEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	payload, err := json.Marshal(map[string]string{"model": e.model, "input": text})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if e.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+e.apiKey)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embeddings request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("embeddings returned %d: %s", resp.StatusCode, body)
	}
	var out struct {
		Data []struct {
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode embeddings: %w", err)
	}
	if len(out.Data) == 0 || len(out.Data[0].Embedding) == 0 {
		return nil, fmt.Errorf("embeddings response has no vector")
	}
	return out.Data[0].Embedding, nil
}

// NewEmbedder builds the embedder named in config.
func NewEmbedder(cfg config.Embedder) Embedder {
	if cfg.Type == config.EmbedderOpenAI {
		return NewOpenAIEmbedder(cfg)
	}
	return NewHashEmbedder(cfg.Dim)
}
