package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/Bhavd33p/llm-gateway/internal/config"
)

// OpenAI talks to any server that implements POST /chat/completions: OpenAI, Groq, Together, vLLM, Ollama and the
// mock provider in this repo.
type OpenAI struct {
	name     string
	endpoint string
	apiKey   string
	modelMap map[string]string
	client   *http.Client
}

func NewOpenAI(u config.Upstream, client *http.Client) *OpenAI {
	return &OpenAI{
		name:     u.Name,
		endpoint: strings.TrimRight(u.BaseURL, "/") + "/chat/completions",
		apiKey:   u.APIKey,
		modelMap: u.ModelMap,
		client:   client,
	}
}

func (p *OpenAI) Name() string { return p.name }

func (p *OpenAI) ModelFor(requested string) string { return modelFor(p.modelMap, requested) }

func (p *OpenAI) Complete(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
	body := *req
	body.Model = p.ModelFor(req.Model)
	body.Stream = false
	body.StreamOptions = nil

	resp, err := p.post(ctx, &body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out ChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("%s: decode response: %w", p.name, err)
	}
	return &out, nil
}

func (p *OpenAI) Stream(ctx context.Context, req *ChatRequest) (Stream, error) {
	body := *req
	body.Model = p.ModelFor(req.Model)
	body.Stream = true
	// Always ask for the final usage chunk so cost accounting uses real token counts. The server only forwards
	// that chunk to clients that asked for it themselves.
	body.StreamOptions = &StreamOptions{IncludeUsage: true}

	resp, err := p.post(ctx, &body)
	if err != nil {
		return nil, err
	}
	return &openAIStream{body: resp.Body, events: newSSEReader(resp.Body)}, nil
}

func (p *OpenAI) post(ctx context.Context, body *ChatRequest) (*http.Response, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if body.Stream {
		httpReq.Header.Set("Accept", "text/event-stream")
	}
	if p.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+p.apiKey)
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

// readStatusError captures a short excerpt of the error body and closes it so the connection can be reused.
func readStatusError(name string, resp *http.Response) error {
	defer resp.Body.Close()
	excerpt, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	return &StatusError{Provider: name, StatusCode: resp.StatusCode, Body: strings.TrimSpace(string(excerpt))}
}

type openAIStream struct {
	body     io.ReadCloser
	events   *sseReader
	done     bool
	finished bool // a chunk with finish_reason arrived
}

func (s *openAIStream) Recv() (*Chunk, error) {
	if s.done {
		return nil, io.EOF
	}
	for {
		ev, err := s.events.Next()
		if err == io.EOF {
			s.done = true
			// Some OpenAI-compatible servers omit [DONE]; a finished generation is still complete.
			if s.finished {
				return nil, io.EOF
			}
			return nil, io.ErrUnexpectedEOF
		}
		if err != nil {
			return nil, err
		}
		if len(ev.Data) == 0 {
			continue
		}
		if bytes.Equal(ev.Data, []byte("[DONE]")) {
			s.done = true
			// Read to EOF so net/http returns the connection to the idle pool instead of closing it.
			_, _ = io.Copy(io.Discard, io.LimitReader(s.body, 4096))
			return nil, io.EOF
		}

		var chunk struct {
			Chunk
			Error *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(ev.Data, &chunk); err != nil {
			return nil, fmt.Errorf("decode stream chunk: %w", err)
		}
		if chunk.Error != nil {
			return nil, fmt.Errorf("upstream stream error: %s", chunk.Error.Message)
		}
		if chunk.FinishReason() != "" {
			s.finished = true
		}
		out := chunk.Chunk
		out.Raw = ev.Data
		return &out, nil
	}
}

func (s *openAIStream) Close() error { return s.body.Close() }
