package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Bhavd33p/llm-gateway/internal/config"
)

func newTestAnthropic(t *testing.T, handler http.HandlerFunc) *Anthropic {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return NewAnthropic(config.Upstream{Name: "claude", BaseURL: srv.URL, APIKey: "ak-test"}, srv.Client())
}

func TestAnthropicRequestTranslation(t *testing.T) {
	p := NewAnthropic(config.Upstream{Name: "claude", ModelMap: map[string]string{"gpt-4o-mini": "claude-haiku-4-5"}}, nil)
	temp := 1.5
	got, err := p.toAnthropicRequest(&ChatRequest{
		Model: "gpt-4o-mini",
		Messages: []Message{
			{Role: "system", Content: "Be brief."},
			{Role: "user", Content: "Hi"},
			{Role: "user", Content: "Are you there?"},
			{Role: "assistant", Content: "Yes."},
		},
		Temperature: &temp,
		Stop:        StopSequences{"END"},
	}, true)
	if err != nil {
		t.Fatalf("toAnthropicRequest: %v", err)
	}
	if got.Model != "claude-haiku-4-5" || got.System != "Be brief." || got.MaxTokens != defaultAnthropicMaxTokens || !got.Stream {
		t.Errorf("unexpected header fields: %+v", got)
	}
	if len(got.Messages) != 2 || got.Messages[0].Content != "Hi\n\nAre you there?" || got.Messages[1].Role != "assistant" {
		t.Errorf("messages not merged by role: %+v", got.Messages)
	}
	if *got.Temperature != 1 || got.StopSequences[0] != "END" {
		t.Errorf("temperature should clamp to 1 and stop map to stop_sequences: %+v", got)
	}

	if _, err := p.toAnthropicRequest(&ChatRequest{Messages: []Message{{Role: "tool", Content: "x"}}}, false); err == nil {
		t.Error("tool role should be rejected")
	}
}

func TestAnthropicStreamTranslation(t *testing.T) {
	events := []string{
		"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"claude-haiku-4-5\",\"usage\":{\"input_tokens\":12,\"output_tokens\":1}}}",
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}",
		"event: ping\ndata: {\"type\":\"ping\"}",
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}",
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\" there\"}}",
		"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}",
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":7}}",
		"event: message_stop\ndata: {\"type\":\"message_stop\"}",
	}
	p := newTestAnthropic(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" || r.Header.Get("x-api-key") != "ak-test" || r.Header.Get("anthropic-version") == "" {
			t.Errorf("unexpected request %s headers=%v", r.URL.Path, r.Header)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for _, e := range events {
			fmt.Fprintf(w, "%s\n\n", e)
		}
	})

	stream, err := p.Stream(context.Background(), &ChatRequest{Model: "claude-haiku-4-5", Messages: []Message{{Role: "user", Content: "Hi"}}})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer stream.Close()

	var chunks []*Chunk
	for {
		c, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		chunks = append(chunks, c)
	}
	// role, "Hello", " there", finish, usage
	if len(chunks) != 5 {
		t.Fatalf("got %d chunks, want 5: %+v", len(chunks), chunks)
	}
	if chunks[0].Choices[0].Delta.Role != "assistant" || chunks[1].Content() != "Hello" || chunks[2].Content() != " there" {
		t.Errorf("content chunks wrong: %+v %+v %+v", chunks[0], chunks[1], chunks[2])
	}
	if chunks[3].FinishReason() != "stop" {
		t.Errorf("finish reason = %q, want stop", chunks[3].FinishReason())
	}
	if u := chunks[4].Usage; u == nil || u.PromptTokens != 12 || u.CompletionTokens != 7 || len(chunks[4].Choices) != 0 {
		t.Errorf("usage chunk wrong: %+v", chunks[4])
	}
	encoded, _ := json.Marshal(chunks[1])
	if !strings.Contains(string(encoded), `"object":"chat.completion.chunk"`) || chunks[1].Raw != nil {
		t.Errorf("translated chunk should encode as an OpenAI chunk: %s", encoded)
	}
}

func TestAnthropicStreamErrorEventIsRetryable(t *testing.T) {
	p := newTestAnthropic(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n")
	})
	stream, err := p.Stream(context.Background(), &ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "Hi"}}})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer stream.Close()
	_, err = stream.Recv()
	var se *StatusError
	if !errors.As(err, &se) || se.StatusCode != 529 || !se.Retryable() {
		t.Fatalf("err = %v, want retryable 529 StatusError", err)
	}
}

func TestAnthropicComplete(t *testing.T) {
	p := newTestAnthropic(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"id":"msg_2","type":"message","role":"assistant","model":"claude-haiku-4-5","content":[{"type":"text","text":"Hi!"}],"stop_reason":"max_tokens","usage":{"input_tokens":4,"output_tokens":2}}`)
	})
	resp, err := p.Complete(context.Background(), &ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "Hi"}}})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if resp.Choices[0].Message.Content != "Hi!" || resp.Choices[0].FinishReason != "length" || resp.Usage.TotalTokens != 6 {
		t.Errorf("unexpected translation: %+v", resp)
	}
}
