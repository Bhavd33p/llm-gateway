// Package provider defines the OpenAI-compatible wire types the gateway speaks and the adapters that talk to
// each upstream API. Every adapter translates to and from these types, so the rest of the gateway never sees a
// provider-specific format.
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
)

// Provider is one upstream API. Stream must return only after the upstream has accepted the request (HTTP 200),
// so that connection errors and error statuses surface here, where the router can still retry them.
type Provider interface {
	Name() string
	// ModelFor returns the model name this upstream should be asked for, after applying its model_map.
	ModelFor(requested string) string
	Complete(ctx context.Context, req *ChatRequest) (*ChatResponse, error)
	Stream(ctx context.Context, req *ChatRequest) (Stream, error)
}

// Stream yields OpenAI-format chunks. Recv returns io.EOF after the last chunk; Close releases the connection.
type Stream interface {
	Recv() (*Chunk, error)
	Close() error
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
	Name    string `json:"name,omitempty"`
}

type StreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

// ChatRequest is the subset of the OpenAI chat completions request the gateway understands.
// Text content only: tool calls and multimodal parts are not translated.
type ChatRequest struct {
	Model         string         `json:"model"`
	Messages      []Message      `json:"messages"`
	Stream        bool           `json:"stream,omitempty"`
	StreamOptions *StreamOptions `json:"stream_options,omitempty"`
	MaxTokens     *int           `json:"max_tokens,omitempty"`
	Temperature   *float64       `json:"temperature,omitempty"`
	TopP          *float64       `json:"top_p,omitempty"`
	Stop          StopSequences  `json:"stop,omitempty"`
	User          string         `json:"user,omitempty"`
}

// StopSequences accepts OpenAI's "stop" as either a single string or an array of strings.
type StopSequences []string

func (s *StopSequences) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, []byte("null")) {
		*s = nil
		return nil
	}
	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*s = StopSequences{single}
		return nil
	}
	var many []string
	if err := json.Unmarshal(data, &many); err != nil {
		return fmt.Errorf("stop must be a string or an array of strings")
	}
	*s = many
	return nil
}

type ChatResponse struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   *Usage   `json:"usage,omitempty"`
}

type Choice struct {
	Index        int     `json:"index"`
	Message      Message `json:"message"`
	FinishReason string  `json:"finish_reason"`
}

type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// Chunk is one "chat.completion.chunk" server-sent event.
type Chunk struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []ChunkChoice `json:"choices"`
	Usage   *Usage        `json:"usage,omitempty"`

	// Raw holds the upstream's original bytes when it already speaks the OpenAI format, so the server can pass the
	// event through unchanged instead of re-encoding it. Nil for translated chunks.
	Raw []byte `json:"-"`
}

type ChunkChoice struct {
	Index        int     `json:"index"`
	Delta        Delta   `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

type Delta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

// Content returns the text delta carried by the chunk, if any.
func (c *Chunk) Content() string {
	if len(c.Choices) == 0 {
		return ""
	}
	return c.Choices[0].Delta.Content
}

// FinishReason returns the chunk's finish reason, or "" while the generation is still running.
func (c *Chunk) FinishReason() string {
	if len(c.Choices) == 0 || c.Choices[0].FinishReason == nil {
		return ""
	}
	return *c.Choices[0].FinishReason
}

// StatusError is a non-200 answer from an upstream. The router retries 429 and 5xx, and returns other codes to
// the client because sending the same bad request elsewhere would fail the same way.
type StatusError struct {
	Provider   string
	StatusCode int
	Body       string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s: upstream returned %d: %s", e.Provider, e.StatusCode, e.Body)
}

func (e *StatusError) Retryable() bool {
	return e.StatusCode == 429 || e.StatusCode >= 500
}

func modelFor(modelMap map[string]string, requested string) string {
	if mapped, ok := modelMap[requested]; ok {
		return mapped
	}
	return requested
}
