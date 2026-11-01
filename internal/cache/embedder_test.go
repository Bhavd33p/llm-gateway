package cache

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Bhavd33p/llm-gateway/internal/config"
)

func TestCosine(t *testing.T) {
	cases := []struct {
		a, b []float32
		want float64
	}{
		{[]float32{1, 2, 3}, []float32{2, 4, 6}, 1},
		{[]float32{1, 0}, []float32{0, 1}, 0},
		{[]float32{1, 1}, []float32{-1, -1}, -1},
		{[]float32{1, 2}, []float32{1, 2, 3}, 0}, // different dimensions
		{[]float32{0, 0}, []float32{1, 1}, 0},    // zero vector
	}
	for _, tc := range cases {
		if got := Cosine(tc.a, tc.b); math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("Cosine(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestVectorEncodingRoundTrip(t *testing.T) {
	in := []float32{0, 1.5, -2.25, float32(math.Pi)}
	out, err := decodeVector(encodeVector(in))
	if err != nil || len(out) != len(in) {
		t.Fatalf("decode: %v %v", out, err)
	}
	for i := range in {
		if in[i] != out[i] {
			t.Fatalf("round trip changed %v to %v", in, out)
		}
	}
	if _, err := decodeVector([]byte{1, 2, 3}); err == nil {
		t.Error("a blob that is not a multiple of 4 bytes should fail")
	}
}

func TestHashEmbedderSimilarity(t *testing.T) {
	e := NewHashEmbedder(256)
	embed := func(s string) []float32 {
		v, _ := e.Embed(context.Background(), s)
		return v
	}
	base := embed("model: gpt-4o-mini\nuser: what is the capital of france")
	if n := Cosine(base, base); math.Abs(n-1) > 1e-6 {
		t.Errorf("self-similarity = %v, want 1", n)
	}
	reworded := embed("model: gpt-4o-mini\nuser: What is the capital of France?")
	if sim := Cosine(base, reworded); sim < 0.999 {
		t.Errorf("case and punctuation changes should not matter, similarity = %v", sim)
	}
	nearby := embed("model: gpt-4o-mini\nuser: what is the capital city of france")
	if sim := Cosine(base, nearby); sim < 0.7 || sim > 0.99 {
		t.Errorf("one extra word should be similar but not identical, similarity = %v", sim)
	}
	unrelated := embed("model: gpt-4o-mini\nuser: write a haiku about kubernetes autoscaling")
	if sim := Cosine(base, unrelated); sim > 0.5 {
		t.Errorf("unrelated prompts should be dissimilar, similarity = %v", sim)
	}
}

func TestOpenAIEmbedder(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/embeddings" || r.Header.Get("Authorization") != "Bearer k" {
			t.Errorf("unexpected request %s", r.URL.Path)
		}
		fmt.Fprint(w, `{"data":[{"embedding":[0.6,0.8]}]}`)
	}))
	defer srv.Close()
	e := NewOpenAIEmbedder(config.Embedder{BaseURL: srv.URL + "/v1", APIKey: "k", Model: "m", Timeout: time.Second})
	v, err := e.Embed(context.Background(), "hi")
	if err != nil || len(v) != 2 || v[0] != 0.6 {
		t.Fatalf("Embed = %v, %v", v, err)
	}
}
