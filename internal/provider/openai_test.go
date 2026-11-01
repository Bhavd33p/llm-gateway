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

func newTestOpenAI(t *testing.T, handler http.HandlerFunc) *OpenAI {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	u := config.Upstream{Name: "test", BaseURL: srv.URL + "/v1", APIKey: "sk-test", ModelMap: map[string]string{"alias": "real-model"}}
	return NewOpenAI(u, srv.Client())
}

func TestOpenAIStreamPassesChunksThrough(t *testing.T) {
	chunks := []string{
		`{"id":"c1","object":"chat.completion.chunk","model":"real-model","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}`,
		`{"id":"c1","object":"chat.completion.chunk","model":"real-model","choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]}`,
		`{"id":"c1","object":"chat.completion.chunk","model":"real-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
		`{"id":"c1","object":"chat.completion.chunk","model":"real-model","choices":[],"usage":{"prompt_tokens":5,"completion_tokens":1,"total_tokens":6}}`,
	}
	var got ChatRequest
	p := newTestOpenAI(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer sk-test" {
			t.Errorf("unexpected request %s auth=%q", r.URL.Path, r.Header.Get("Authorization"))
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.Header().Set("Content-Type", "text/event-stream")
		for _, c := range chunks {
			fmt.Fprintf(w, "data: %s\n\n", c)
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	})

	stream, err := p.Stream(context.Background(), &ChatRequest{Model: "alias", Messages: []Message{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer stream.Close()

	if got.Model != "real-model" || !got.Stream || got.StreamOptions == nil || !got.StreamOptions.IncludeUsage {
		t.Errorf("upstream request = %+v, want mapped model, stream=true and include_usage", got)
	}

	var raws []string
	var content strings.Builder
	var usage *Usage
	for {
		c, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		raws = append(raws, string(c.Raw))
		content.WriteString(c.Content())
		if c.Usage != nil {
			usage = c.Usage
		}
	}
	if strings.Join(raws, "|") != strings.Join(chunks, "|") {
		t.Errorf("raw chunks were not passed through unchanged:\n got %v\nwant %v", raws, chunks)
	}
	if content.String() != "Hello" || usage == nil || usage.TotalTokens != 6 {
		t.Errorf("content=%q usage=%+v", content.String(), usage)
	}
}

func TestOpenAIStreamWithoutDoneIsUnexpectedEOF(t *testing.T) {
	p := newTestOpenAI(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `data: {"choices":[{"delta":{"content":"partial"},"finish_reason":null}]}`+"\n\n")
	})
	stream, err := p.Stream(context.Background(), &ChatRequest{Model: "m"})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer stream.Close()
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("first Recv: %v", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestOpenAIErrorStatus(t *testing.T) {
	for _, tc := range []struct {
		status    int
		retryable bool
	}{{http.StatusServiceUnavailable, true}, {http.StatusTooManyRequests, true}, {http.StatusBadRequest, false}} {
		p := newTestOpenAI(t, func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, `{"error":{"message":"nope"}}`, tc.status)
		})
		_, err := p.Complete(context.Background(), &ChatRequest{Model: "m"})
		var se *StatusError
		if !errors.As(err, &se) || se.StatusCode != tc.status || se.Retryable() != tc.retryable {
			t.Errorf("status %d: err = %v, want StatusError retryable=%v", tc.status, err, tc.retryable)
		}
	}
}

func TestOpenAIComplete(t *testing.T) {
	p := newTestOpenAI(t, func(w http.ResponseWriter, r *http.Request) {
		var req ChatRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Stream || req.StreamOptions != nil {
			t.Errorf("non-streaming request should not ask for a stream: %+v", req)
		}
		fmt.Fprint(w, `{"id":"x","object":"chat.completion","model":"real-model","choices":[{"index":0,"message":{"role":"assistant","content":"Hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}`)
	})
	resp, err := p.Complete(context.Background(), &ChatRequest{Model: "alias", Stream: true})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if resp.Choices[0].Message.Content != "Hi" || resp.Usage.TotalTokens != 4 {
		t.Errorf("unexpected response %+v", resp)
	}
}

func TestStopSequencesAcceptsStringOrArray(t *testing.T) {
	var a, b ChatRequest
	if err := json.Unmarshal([]byte(`{"stop":"END"}`), &a); err != nil || len(a.Stop) != 1 || a.Stop[0] != "END" {
		t.Errorf("string form: %v %v", a.Stop, err)
	}
	if err := json.Unmarshal([]byte(`{"stop":["A","B"]}`), &b); err != nil || len(b.Stop) != 2 {
		t.Errorf("array form: %v %v", b.Stop, err)
	}
}
