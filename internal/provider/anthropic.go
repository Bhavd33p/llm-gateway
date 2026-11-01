package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/Bhavd33p/llm-gateway/internal/config"
)

const (
	anthropicVersion = "2023-06-01"
	// Anthropic requires max_tokens; OpenAI does not, so a default is needed when the client omits it.
	defaultAnthropicMaxTokens = 1024
)

// Anthropic adapts the Anthropic Messages API to the OpenAI chat completions format, in both directions.
type Anthropic struct {
	name     string
	endpoint string
	apiKey   string
	modelMap map[string]string
	client   *http.Client
}

func NewAnthropic(u config.Upstream, client *http.Client) *Anthropic {
	return &Anthropic{
		name:     u.Name,
		endpoint: strings.TrimRight(u.BaseURL, "/") + "/v1/messages",
		apiKey:   u.APIKey,
		modelMap: u.ModelMap,
		client:   client,
	}
}

func (p *Anthropic) Name() string { return p.name }

func (p *Anthropic) ModelFor(requested string) string { return modelFor(p.modelMap, requested) }

type anthropicRequest struct {
	Model         string             `json:"model"`
	System        string             `json:"system,omitempty"`
	Messages      []anthropicMessage `json:"messages"`
	MaxTokens     int                `json:"max_tokens"`
	Temperature   *float64           `json:"temperature,omitempty"`
	TopP          *float64           `json:"top_p,omitempty"`
	StopSequences []string           `json:"stop_sequences,omitempty"`
	Stream        bool               `json:"stream,omitempty"`
}

type anthropicMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type anthropicUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

type anthropicResponse struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	StopReason string         `json:"stop_reason"`
	Usage      anthropicUsage `json:"usage"`
}

// toAnthropicRequest moves system messages into the top-level system field and merges consecutive messages from
// the same role, which the Messages API expects to alternate.
func (p *Anthropic) toAnthropicRequest(req *ChatRequest, stream bool) (*anthropicRequest, error) {
	out := &anthropicRequest{
		Model:         p.ModelFor(req.Model),
		MaxTokens:     defaultAnthropicMaxTokens,
		TopP:          req.TopP,
		StopSequences: req.Stop,
		Stream:        stream,
	}
	if req.MaxTokens != nil {
		out.MaxTokens = *req.MaxTokens
	}
	if req.Temperature != nil {
		t := math.Min(*req.Temperature, 1) // OpenAI accepts 0..2, Anthropic 0..1
		out.Temperature = &t
	}

	var system []string
	for _, m := range req.Messages {
		switch m.Role {
		case "system", "developer":
			system = append(system, m.Content)
		case "user", "assistant":
			if n := len(out.Messages); n > 0 && out.Messages[n-1].Role == m.Role {
				out.Messages[n-1].Content += "\n\n" + m.Content
				continue
			}
			out.Messages = append(out.Messages, anthropicMessage{Role: m.Role, Content: m.Content})
		default:
			return nil, &StatusError{Provider: p.name, StatusCode: http.StatusBadRequest, Body: fmt.Sprintf("role %q is not supported by the anthropic adapter", m.Role)}
		}
	}
	out.System = strings.Join(system, "\n\n")
	return out, nil
}

func (p *Anthropic) Complete(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
	body, err := p.toAnthropicRequest(req, false)
	if err != nil {
		return nil, err
	}
	resp, err := p.post(ctx, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var ar anthropicResponse
	if err := json.NewDecoder(resp.Body).Decode(&ar); err != nil {
		return nil, fmt.Errorf("%s: decode response: %w", p.name, err)
	}
	var text strings.Builder
	for _, block := range ar.Content {
		if block.Type == "text" {
			text.WriteString(block.Text)
		}
	}
	return &ChatResponse{
		ID:      ar.ID,
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   ar.Model,
		Choices: []Choice{{
			Message:      Message{Role: "assistant", Content: text.String()},
			FinishReason: finishReason(ar.StopReason),
		}},
		Usage: toUsage(ar.Usage),
	}, nil
}

func (p *Anthropic) Stream(ctx context.Context, req *ChatRequest) (Stream, error) {
	body, err := p.toAnthropicRequest(req, true)
	if err != nil {
		return nil, err
	}
	resp, err := p.post(ctx, body)
	if err != nil {
		return nil, err
	}
	return &anthropicStream{
		name:    p.name,
		body:    resp.Body,
		events:  newSSEReader(resp.Body),
		created: time.Now().Unix(),
	}, nil
}

func (p *Anthropic) post(ctx context.Context, body *anthropicRequest) (*http.Response, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("anthropic-version", anthropicVersion)
	if p.apiKey != "" {
		httpReq.Header.Set("x-api-key", p.apiKey)
	}
	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, readStatusError(p.name, resp)
	}
	return resp, nil
}

// anthropicStream translates Anthropic's typed SSE events into OpenAI chunks:
//
//	message_start        -> chunk with delta.role = "assistant"
//	content_block_delta  -> chunk with delta.content
//	message_delta        -> chunk with finish_reason, then a usage-only chunk (like OpenAI's include_usage)
//	message_stop         -> io.EOF
//	error                -> error (retryable if it is an overload or server error)
type anthropicStream struct {
	name    string
	body    io.ReadCloser
	events  *sseReader
	created int64

	id      string
	model   string
	usage   anthropicUsage
	pending *Chunk // usage chunk queued behind the finish chunk
	done    bool
}

func (s *anthropicStream) Recv() (*Chunk, error) {
	if s.pending != nil {
		c := s.pending
		s.pending = nil
		return c, nil
	}
	if s.done {
		return nil, io.EOF
	}
	for {
		ev, err := s.events.Next()
		if err == io.EOF {
			return nil, io.ErrUnexpectedEOF // the stream must end with message_stop
		}
		if err != nil {
			return nil, err
		}

		switch ev.Event {
		case "message_start":
			var payload struct {
				Message struct {
					ID    string         `json:"id"`
					Model string         `json:"model"`
					Usage anthropicUsage `json:"usage"`
				} `json:"message"`
			}
			if err := json.Unmarshal(ev.Data, &payload); err != nil {
				return nil, fmt.Errorf("decode message_start: %w", err)
			}
			s.id, s.model = payload.Message.ID, payload.Message.Model
			s.usage.InputTokens = payload.Message.Usage.InputTokens
			return s.chunk(Delta{Role: "assistant"}, nil), nil

		case "content_block_delta":
			var payload struct {
				Delta struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"delta"`
			}
			if err := json.Unmarshal(ev.Data, &payload); err != nil {
				return nil, fmt.Errorf("decode content_block_delta: %w", err)
			}
			if payload.Delta.Type != "text_delta" {
				continue
			}
			return s.chunk(Delta{Content: payload.Delta.Text}, nil), nil

		case "message_delta":
			var payload struct {
				Delta struct {
					StopReason string `json:"stop_reason"`
				} `json:"delta"`
				Usage anthropicUsage `json:"usage"`
			}
			if err := json.Unmarshal(ev.Data, &payload); err != nil {
				return nil, fmt.Errorf("decode message_delta: %w", err)
			}
			s.usage.OutputTokens = payload.Usage.OutputTokens
			reason := finishReason(payload.Delta.StopReason)
			s.pending = &Chunk{
				ID: s.id, Object: "chat.completion.chunk", Created: s.created, Model: s.model,
				Choices: []ChunkChoice{},
				Usage:   toUsage(s.usage),
			}
			return s.chunk(Delta{}, &reason), nil

		case "message_stop":
			s.done = true
			_, _ = io.Copy(io.Discard, io.LimitReader(s.body, 4096))
			if s.pending != nil {
				return s.Recv()
			}
			return nil, io.EOF

		case "error":
			var payload struct {
				Error struct {
					Type    string `json:"type"`
					Message string `json:"message"`
				} `json:"error"`
			}
			_ = json.Unmarshal(ev.Data, &payload)
			return nil, &StatusError{Provider: s.name, StatusCode: anthropicErrorStatus(payload.Error.Type), Body: payload.Error.Message}

		default: // ping, content_block_start, content_block_stop
			continue
		}
	}
}

func (s *anthropicStream) Close() error { return s.body.Close() }

func (s *anthropicStream) chunk(delta Delta, finish *string) *Chunk {
	return &Chunk{
		ID: s.id, Object: "chat.completion.chunk", Created: s.created, Model: s.model,
		Choices: []ChunkChoice{{Delta: delta, FinishReason: finish}},
	}
}

func finishReason(stopReason string) string {
	switch stopReason {
	case "max_tokens":
		return "length"
	case "tool_use":
		return "tool_calls"
	default: // end_turn, stop_sequence
		return "stop"
	}
}

func toUsage(u anthropicUsage) *Usage {
	return &Usage{PromptTokens: u.InputTokens, CompletionTokens: u.OutputTokens, TotalTokens: u.InputTokens + u.OutputTokens}
}

// anthropicErrorStatus maps an in-stream error type to the HTTP status Anthropic would have used.
func anthropicErrorStatus(errType string) int {
	switch errType {
	case "overloaded_error":
		return 529
	case "rate_limit_error":
		return http.StatusTooManyRequests
	case "invalid_request_error":
		return http.StatusBadRequest
	default:
		return http.StatusBadGateway
	}
}
