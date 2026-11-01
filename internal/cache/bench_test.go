package cache

import (
	"context"
	"testing"
)

func BenchmarkHashEmbed(b *testing.B) {
	e := NewHashEmbedder(256)
	text := "model: gpt-4o-mini\nuser: explain how connection pooling reduces latency for an llm gateway under load"
	for b.Loop() {
		e.Embed(context.Background(), text)
	}
}

// BenchmarkCosine256 is the per-candidate cost of the linear scan; a lookup over N entries costs about N of these.
func BenchmarkCosine256(b *testing.B) {
	e := NewHashEmbedder(256)
	x, _ := e.Embed(context.Background(), "what is the capital of france")
	y, _ := e.Embed(context.Background(), "what is the capital city of france")
	for b.Loop() {
		Cosine(x, y)
	}
}
