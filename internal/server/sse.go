package server

import (
	"encoding/json"
	"net/http"

	"github.com/Bhavd33p/llm-gateway/internal/provider"
)

// sseWriter writes server-sent events and flushes after each one, so every chunk leaves the gateway as soon as it
// arrives instead of waiting in a buffer.
type sseWriter struct {
	w       http.ResponseWriter
	flusher http.Flusher
	buf     []byte
}

func newSSEWriter(w http.ResponseWriter) *sseWriter {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no") // ask nginx-style proxies in front of us not to buffer the stream
	flusher, _ := w.(http.Flusher)
	return &sseWriter{w: w, flusher: flusher}
}

// chunk forwards the upstream's original bytes when it has them, and encodes translated chunks otherwise.
func (s *sseWriter) chunk(c *provider.Chunk) error {
	data := c.Raw
	if data == nil {
		var err error
		if data, err = json.Marshal(c); err != nil {
			return err
		}
	}
	return s.event(data)
}

func (s *sseWriter) done() error {
	return s.event([]byte("[DONE]"))
}

// streamError tells the client in-band that the stream broke; after the first byte the status code is already sent.
func (s *sseWriter) streamError(message string) error {
	var body apiError
	body.Error.Message, body.Error.Type = message, "upstream_error"
	data, _ := json.Marshal(body)
	return s.event(data)
}

func (s *sseWriter) event(data []byte) error {
	s.buf = append(s.buf[:0], "data: "...)
	s.buf = append(s.buf, data...)
	s.buf = append(s.buf, "\n\n"...)
	if _, err := s.w.Write(s.buf); err != nil {
		return err
	}
	if s.flusher != nil {
		s.flusher.Flush()
	}
	return nil
}
