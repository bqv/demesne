// stream.go -- the streaming chat path, and the two things a cold load needs.
//
// Two failures, both measured on 2026-11-07 (session d9ff4d92 and the estate log):
//
//  1. A client's HTTP header deadline. llama.cpp's router does not answer a chat request
//     until the rung it needs is loaded, and demesne forwarded headers only when the upstream
//     sent them -- so for the whole of a 27B cold load the caller had received nothing, and a
//     300 s transport timeout fired at 302 s. demesne now answers the headers itself, before
//     dialling, and keeps the stream warm with SSE comments while the engine loads. A reply
//     that can no longer be a status is an `event: error` in the stream instead, which is the
//     one semantic price of answering early -- and it is paid only by requests that ask to
//     stream, which are exactly the requests a cold load used to kill.
//
//  2. A load is SHARED state, not the caller's private work. The client's cancellation is
//     therefore not propagated to the engine until the engine has answered: before that, the
//     request is kept alive deliberately and the caller is forgotten. That is what stops one
//     impatient client from destroying a load every other client is waiting for.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// keepAliveEvery is how often a waiting stream says something. SSE comments are ignored by
// every parser, so this is a heartbeat rather than a message: it exists so that clients and
// any intermediate proxy can see the connection is alive and no header deadline fires.
const keepAliveEvery = 10 * time.Second

var errClientGone = errors.New("the client went away")

// sse is a streaming response: one writer, guarded, because the heartbeat and the body copy
// run concurrently and an http.ResponseWriter is not safe for that on its own.
type sse struct {
	mu sync.Mutex
	w  http.ResponseWriter
	f  http.Flusher
}

func newSSE(w http.ResponseWriter) *sse {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	h.Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	s := &sse{w: w}
	if f, ok := w.(http.Flusher); ok {
		s.f = f
	}
	s.write(": demesne: streaming\n\n")
	return s
}

func (s *sse) write(text string) {
	s.mu.Lock()
	_, _ = io.WriteString(s.w, text)
	if s.f != nil {
		s.f.Flush()
	}
	s.mu.Unlock()
}

func (s *sse) comment(text string) { s.write(": " + text + "\n\n") }

// event is the only way left to report a problem once 200 has gone out.
func (s *sse) event(name string, payload any) {
	b, err := json.Marshal(payload)
	if err != nil {
		return
	}
	s.write("event: " + name + "\ndata: " + string(b) + "\n\n")
}

// copyFrom relays the engine's stream verbatim, flushing as it goes, so a token is never held
// back by a buffer.
func (s *sse) copyFrom(src io.Reader) error {
	buf := make([]byte, 16<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			s.mu.Lock()
			_, werr := s.w.Write(buf[:n])
			if s.f != nil {
				s.f.Flush()
			}
			s.mu.Unlock()
			if werr != nil {
				return werr
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

// streamUpstream answers the client, then asks the engine. The returned cleanup must be
// deferred; it stops the heartbeat and releases the upstream context and body.
func (d *demesne) streamUpstream(w http.ResponseWriter, r *http.Request, url string, body []byte) (*sse, *http.Response, func(), error) {
	out := newSSE(w)

	// Deliberately NOT r.Context(): see the file comment. This context is cancelled only when
	// the engine has answered and the caller is gone, or when the caller leaves mid-stream.
	ctx, cancel := context.WithCancel(context.Background())
	var answered atomic.Bool

	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(keepAliveEvery)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				out.comment("demesne: the engine is still loading")
			}
		}
	}()

	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			close(stop)
			cancel()
		})
	}

	go func() {
		<-r.Context().Done()
		if answered.Load() {
			cancel()
		}
	}()

	req, err := http.NewRequestWithContext(ctx, r.Method, url, bytes.NewReader(body))
	if err != nil {
		cleanup()
		out.event("error", map[string]any{"message": err.Error(), "code": http.StatusInternalServerError})
		return out, nil, cleanup, err
	}
	if ct := r.Header.Get("Content-Type"); ct != "" {
		req.Header.Set("Content-Type", ct)
	}
	if auth := r.Header.Get("Authorization"); auth != "" {
		req.Header.Set("Authorization", auth)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cleanup()
		if r.Context().Err() == nil {
			out.event("error", map[string]any{
				"message": "the engine did not answer: " + err.Error(),
				"code":    http.StatusBadGateway,
			})
		}
		return out, nil, cleanup, err
	}
	answered.Store(true)

	// The caller gave up while we were waiting: let its load finish (it is shared work) but
	// do not stream the answer into a closed connection.
	if r.Context().Err() != nil {
		resp.Body.Close()
		cleanup()
		return out, nil, cleanup, errors.New("the client went away while the engine was loading")
	}

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		cleanup()
		out.event("error", map[string]any{
			"message": engineMessage(raw),
			"code":    resp.StatusCode,
		})
		return out, nil, cleanup, errors.New(engineMessage(raw))
	}
	return out, resp, cleanup, nil
}

// streamChat is the chat path when the caller asked for a stream: answer, then relay.
func (d *demesne) streamChat(w http.ResponseWriter, r *http.Request, s *slot, body []byte) {
	url := strings.TrimRight(s.specOf().url, "/") + r.URL.Path
	out, resp, done, err := d.streamUpstream(w, r, url, body)
	defer done()
	if err != nil || resp == nil {
		return
	}
	defer resp.Body.Close()
	if err := out.copyFrom(resp.Body); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, errClientGone) {
			log.Printf("%s %s: the client went away mid-stream; the engine keeps its work",
				r.Method, r.URL.Path)
			return
		}
		out.comment("demesne: the engine stream ended: " + err.Error())
	}
}

// engineMessage digs the human reason out of an engine's error body, whichever of the two
// shapes it used.
func engineMessage(raw []byte) string {
	var upstream struct {
		Message string `json:"message"`
		Error   struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(raw, &upstream) == nil {
		if upstream.Message != "" {
			return upstream.Message
		}
		if upstream.Error.Message != "" {
			return upstream.Error.Message
		}
	}
	msg := strings.TrimSpace(string(raw))
	if len(msg) > 400 {
		msg = msg[:400]
	}
	if msg == "" {
		msg = "the engine refused the request"
	}
	return msg
}
