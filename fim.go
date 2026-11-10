// fim.go -- Mistral's completions dialect, on the front door.
//
// Mistral's /v1/fim/completions is the only widely-implemented NAME for the thing
// llama.cpp calls /infill: send a prefix and a suffix, get the text that goes between them.
// Their chat endpoint is OpenAI-shaped and needs nothing from us; this one is worth an
// adapter because no OpenAI-shaped client can reach /infill at all.
//
// The translation is three parts, and the third is the one that costs: a field rename
// (prompt/suffix -> input_prefix/input_suffix), a `model` field added because the router
// picks the rung from it, and a RESPONSE enclosure -- upstream /infill is stream-shaped and
// not OAI-compatible, so the reply is re-wrapped into a chat.completion object (or OpenAI
// SSE chunks when the caller asks to stream).
//
// The model still has to carry FIM tokens (fim_pre/fim_suf/fim_mid), and that is llama.cpp's
// condition, not ours: a model without them answers 501 "Infill is not supported by this
// model", which is passed through as-is because it is the true answer.

package main

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
)

// fimRequest is the Mistral shape, and deliberately only the fields that have an upstream
// meaning. A field nobody can honour should fail loudly rather than be accepted and dropped.
type fimRequest struct {
	Model       string          `json:"model"`
	Prompt      string          `json:"prompt"`
	Suffix      string          `json:"suffix"`
	MaxTokens   int             `json:"max_tokens"`
	Temperature *float64        `json:"temperature"`
	TopP        *float64        `json:"top_p"`
	Seed        *int            `json:"random_seed"`
	Stop        json.RawMessage `json:"stop"`
	Stream      bool            `json:"stream"`
}

// fimUpstream is what llama.cpp's /infill takes. n_predict is the token budget, and `stream`
// is always true on our side: the upstream's streaming path is the one every build has, and
// accumulating it here is cheaper than depending on which builds honour stream:false.
type fimUpstream struct {
	Model       string          `json:"model,omitempty"`
	InputPrefix string          `json:"input_prefix"`
	InputSuffix string          `json:"input_suffix"`
	NPredict    int             `json:"n_predict"`
	Temperature *float64        `json:"temperature,omitempty"`
	TopP        *float64        `json:"top_p,omitempty"`
	Seed        *int            `json:"seed,omitempty"`
	Stop        json.RawMessage `json:"stop,omitempty"`
	Stream      bool            `json:"stream"`
}

func (d *demesne) handleFIM(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMistralError(w, http.StatusMethodNotAllowed, "POST required", "invalid_request_error")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, modelBodyCap))
	if err != nil {
		writeMistralError(w, http.StatusBadRequest, "could not read the request", "invalid_request_error")
		return
	}
	var in fimRequest
	if err := json.Unmarshal(body, &in); err != nil {
		writeMistralError(w, http.StatusUnprocessableEntity, "body is not a FIM request: "+err.Error(), "invalid_request_error")
		return
	}
	if in.Prompt == "" && in.Suffix == "" {
		writeMistralError(w, http.StatusUnprocessableEntity, "prompt or suffix is required", "invalid_request_error")
		return
	}

	tenant := d.tenantByName("llama")
	if name := d.tenantForModel(in.Model); name != "" {
		tenant = d.tenantByName(name)
	}
	if tenant == nil {
		writeMistralError(w, http.StatusNotFound, "no such tenant", "invalid_request_error")
		return
	}
	if !d.claimCard(w, r, tenant) {
		return
	}

	budget := in.MaxTokens
	if budget <= 0 {
		budget = 128
	}
	up := fimUpstream{
		Model: in.Model, InputPrefix: in.Prompt, InputSuffix: in.Suffix,
		NPredict: budget, Temperature: in.Temperature, TopP: in.TopP,
		Seed: in.Seed, Stop: in.Stop, Stream: true,
	}
	payload, err := json.Marshal(up)
	if err != nil {
		writeMistralError(w, http.StatusInternalServerError, err.Error(), "internal_error")
		return
	}
	id := newCompletionID()
	model := in.Model
	if model == "" {
		model = tenant.specOf().name
	}

	// Streaming goes first: it answers the client before dialling, so a cold load cannot sit
	// behind a header deadline. The non-streaming path below can afford to wait for a status.
	if in.Stream {
		d.streamFIM(w, r, tenant, payload, id, model)
		return
	}

	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost,
		strings.TrimRight(tenant.specOf().url, "/")+"/infill", strings.NewReader(string(payload)))
	if err != nil {
		writeMistralError(w, http.StatusInternalServerError, err.Error(), "internal_error")
		return
	}
	req.Header.Set("Content-Type", "application/json")

	// No client timeout: a completion is bounded by max_tokens, and the caller's own
	// cancellation is the only thing that should end it early.
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		writeMistralError(w, http.StatusBadGateway, "the "+tenant.specOf().name+" engine did not answer: "+err.Error(), "server_error")
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		writeMistralRawError(w, resp.StatusCode, msg)
		return
	}

	d.collectFIM(w, resp.Body, id, model)
}

// streamFIM relays upstream deltas as OpenAI-shaped SSE chunks, which is the dialect Mistral
// documents for a streaming FIM request: data-only events terminated by [DONE]. It answers the
// client first, for the same reason chat does: a cold load must not sit behind a header
// deadline.
func (d *demesne) streamFIM(w http.ResponseWriter, r *http.Request, s *tenant, payload []byte, id, model string) {
	url := strings.TrimRight(s.specOf().url, "/") + "/infill"
	out, resp, done, err := d.streamUpstream(w, r, url, payload, s)
	defer done()
	if err != nil || resp == nil {
		return
	}
	defer resp.Body.Close()
	created := time.Now().Unix()
	emit := func(payload any) bool {
		b, err := json.Marshal(payload)
		if err != nil {
			return false
		}
		out.write("data: " + string(b) + "\n\n")
		return true
	}
	_ = readInfill(resp.Body, func(text string, finished bool) bool {
		if finished {
			return emit(map[string]any{
				"id": id, "object": "chat.completion.chunk", "created": created, "model": model,
				"choices": []any{map[string]any{
					"index": 0, "delta": map[string]any{}, "finish_reason": "stop",
				}},
			})
		}
		return emit(map[string]any{
			"id": id, "object": "chat.completion.chunk", "created": created, "model": model,
			"choices": []any{map[string]any{
				"index": 0, "delta": map[string]any{"content": text}, "finish_reason": nil,
			}},
		})
	})
	out.write("data: [DONE]\n\n")
}

// collectFIM drains the same stream and answers with the one object a non-streaming FIM
// request expects: a chat.completion whose single choice carries the text.
func (d *demesne) collectFIM(w http.ResponseWriter, upstream io.Reader, id, model string) {
	var text strings.Builder
	tokens := 0
	_ = readInfill(upstream, func(chunk string, done bool) bool {
		if done {
			return true
		}
		text.WriteString(chunk)
		tokens++
		return true
	})
	out := map[string]any{
		"id": id, "object": "chat.completion", "created": time.Now().Unix(), "model": model,
		"choices": []any{map[string]any{
			"index": 0,
			"message": map[string]any{
				"role": "assistant", "content": text.String(), "tool_calls": nil, "prefix": false,
			},
			"finish_reason": "stop",
		}},
	}
	if tokens > 0 {
		out["usage"] = map[string]any{
			"prompt_tokens": 0, "completion_tokens": tokens, "total_tokens": tokens,
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// readInfill parses both shapes llama.cpp will hand back: a JSON body with a content field
// (a build that honours stream:false) and the SSE stream of {"content":"..."} events ending
// in [DONE], which is what every build has. The caller decides what to do with the text.
func readInfill(upstream io.Reader, sink func(text string, done bool) bool) error {
	br := bufio.NewReaderSize(upstream, 64<<10)
	peek, _ := br.Peek(512)
	if trimmed := strings.TrimLeft(string(peek), " \t\r\n"); strings.HasPrefix(trimmed, "{") {
		var doc struct {
			Content string `json:"content"`
		}
		if err := json.NewDecoder(br).Decode(&doc); err == nil {
			if doc.Content != "" && !sink(doc.Content, false) {
				return nil
			}
			sink("", true)
			return nil
		}
	}
	for {
		line, err := br.ReadString('\n')
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "data:") {
			payload := strings.TrimSpace(strings.TrimPrefix(trimmed, "data:"))
			if payload == "[DONE]" {
				sink("", true)
				return nil
			}
			var evt struct {
				Content string `json:"content"`
				Stop    bool   `json:"stop"`
			}
			if json.Unmarshal([]byte(payload), &evt) == nil {
				if evt.Content != "" && !sink(evt.Content, false) {
					return nil
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				sink("", true)
				return nil
			}
			return err
		}
	}
}

func newCompletionID() string {
	b := make([]byte, 12)
	for i := range b {
		b[i] = byte(time.Now().UnixNano() >> (i % 8))
	}
	return hex.EncodeToString(b)
}

// writeMistralError answers in the error object Mistral's own API uses, so a client that
// already handles their errors handles ours.
func writeMistralError(w http.ResponseWriter, status int, msg, kind string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"object": "error", "message": msg, "type": kind, "code": status,
	})
}

// writeMistralRawError passes an engine's own error through, re-wrapped: the message is the
// engine's, because it is the only part that says what is actually wrong.
func writeMistralRawError(w http.ResponseWriter, status int, raw []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"object": "error", "message": engineMessage(raw), "type": "server_error", "code": status,
	})
}
