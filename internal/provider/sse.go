package provider

import (
	"bufio"
	"bytes"
	"io"
)

// sseEvent is one server-sent event: an optional "event:" name and its "data:" payload.
type sseEvent struct {
	Event string
	Data  []byte
}

type sseReader struct {
	scanner *bufio.Scanner
}

func newSSEReader(r io.Reader) *sseReader {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20) // a single large chunk must not break the stream
	return &sseReader{scanner: scanner}
}

// Next returns the next complete event, or io.EOF when the body ends.
func (r *sseReader) Next() (sseEvent, error) {
	var ev sseEvent
	var data [][]byte
	pending := false
	for r.scanner.Scan() {
		line := r.scanner.Bytes()
		if len(line) == 0 {
			if pending {
				ev.Data = bytes.Join(data, []byte("\n"))
				return ev, nil
			}
			continue
		}
		if line[0] == ':' { // comment, often used as a keep-alive
			continue
		}
		field, value, _ := bytes.Cut(line, []byte(":"))
		value = bytes.TrimPrefix(value, []byte(" "))
		switch string(field) {
		case "event":
			ev.Event = string(value)
			pending = true
		case "data":
			data = append(data, bytes.Clone(value)) // the scanner reuses its buffer
			pending = true
		}
	}
	if err := r.scanner.Err(); err != nil {
		return sseEvent{}, err
	}
	if pending {
		ev.Data = bytes.Join(data, []byte("\n"))
		return ev, nil
	}
	return sseEvent{}, io.EOF
}
