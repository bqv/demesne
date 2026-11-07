// rpc.go -- the typed control plane: JSON-RPC 2.0 on the way in we already have.
//
// The methods are the point, not the framing. A caller that wants to know "who holds the card"
// should not have to know a URL, a query string and which of four documents answers it; it
// should be able to say estate.card and get a typed answer. That is what ward's own daemon
// does for its own surface (session.prompt, session.get), so the naming here matches it: a
// dotted lower-case verb, params in, one result out.
//
// The transport stays HTTP, deliberately. It is already on the socket, ward's client already
// speaks it (CURLOPT_UNIX_SOCKET_PATH), and Go's standard library has no CBOR -- so a second
// binary framing would mean either a dependency in a stdlib-only daemon or a hand-rolled
// codec, for a payload that is a few hundred bytes. What changes is the vocabulary, not the
// pipes.
//
// Errors follow JSON-RPC 2.0: -32600 invalid request, -32601 no such method, -32602 invalid
// params, -32603 internal error. A refusal that has a better explanation carries it in `data`.

package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
)

const (
	rpcInvalidRequest = -32600
	rpcNoMethod       = -32601
	rpcBadParams      = -32602
	rpcInternal       = -32603
)

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

// slotParams is the one parameter shape several methods take.
type slotParams struct {
	Slot  string `json:"slot"`
	Group string `json:"group"`
}

// handleRPC answers one request, or a batch, or nothing for a notification. A notification is
// a request with no id: it is executed and not answered, which is what JSON-RPC says and what
// a fire-and-forget reload wants.
func (d *demesne) handleRPC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "demesne: POST required", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, modelBodyCap))
	if err != nil {
		writeRPC(w, rpcResponse{JSONRPC: "2.0", Error: &rpcError{Code: rpcInvalidRequest, Message: err.Error()}})
		return
	}

	trimmed := bytes.TrimSpace(body)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		var batch []rpcRequest
		if err := json.Unmarshal(body, &batch); err != nil {
			writeRPC(w, rpcResponse{JSONRPC: "2.0", Error: &rpcError{Code: rpcInvalidRequest, Message: err.Error()}})
			return
		}
		out := make([]rpcResponse, 0, len(batch))
		for _, req := range batch {
			if resp, answered := d.oneRPC(req); answered {
				out = append(out, resp)
			}
		}
		if len(out) == 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeRPC(w, out)
		return
	}

	var req rpcRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeRPC(w, rpcResponse{JSONRPC: "2.0", Error: &rpcError{Code: rpcInvalidRequest, Message: err.Error()}})
		return
	}
	resp, answered := d.oneRPC(req)
	if !answered {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeRPC(w, resp)
}

func writeRPC(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

// oneRPC runs one method. The second return says whether an answer is due at all.
func (d *demesne) oneRPC(req rpcRequest) (rpcResponse, bool) {
	resp := rpcResponse{JSONRPC: "2.0", ID: req.ID}
	notification := len(req.ID) == 0

	result, rerr := d.rpcCall(req.Method, req.Params)
	if rerr != nil {
		resp.Error = rerr
	} else {
		resp.Result = result
	}
	if notification {
		return resp, false
	}
	return resp, true
}

// rpcCall is the whole vocabulary in one place.
func (d *demesne) rpcCall(method string, raw json.RawMessage) (any, *rpcError) {
	var p slotParams
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, &rpcError{Code: rpcBadParams, Message: "params: " + err.Error()}
		}
	}

	switch method {
	case "estate.status":
		slots := d.slotStatuses()
		return map[string]any{"version": version, "slots": slots}, nil

	case "estate.card":
		group := p.Group
		if group == "" {
			group = "gpu"
		}
		return d.cardDocument(group), nil

	case "estate.reload":
		rep, err := d.reload()
		if err != nil {
			return nil, &rpcError{Code: rpcInternal, Message: err.Error()}
		}
		return rep, nil

	case "slots.status":
		s := d.slotByName(p.Slot)
		if s == nil {
			return nil, d.unknownSlot(p.Slot)
		}
		return slotStatusOf(s), nil

	case "slots.load":
		s := d.slotByName(p.Slot)
		if s == nil {
			return nil, d.unknownSlot(p.Slot)
		}
		if err := d.ensure(s); err != nil {
			return nil, &rpcError{Code: rpcInternal, Message: err.Error()}
		}
		return map[string]any{"slot": s.specOf().name, "state": "running"}, nil

	case "slots.reclaim":
		s := d.slotByName(p.Slot)
		if s == nil {
			return nil, d.unknownSlot(p.Slot)
		}
		killed, err := d.reclaimSlot(s)
		if err != nil {
			return nil, &rpcError{Code: rpcInternal, Message: err.Error()}
		}
		return map[string]any{"slot": s.specOf().name, "reclaimed": killed}, nil

	case "slots.stop":
		s := d.slotByName(p.Slot)
		if s == nil {
			return nil, d.unknownSlot(p.Slot)
		}
		d.evict(s, stopOperator, "operator stop over the control plane")
		return map[string]any{"slot": s.specOf().name, "state": "stopped"}, nil
	}
	return nil, &rpcError{Code: rpcNoMethod, Message: "no such method: " + method,
		Data: []string{"estate.status", "estate.card", "estate.reload", "slots.status", "slots.load", "slots.stop"}}
}

func (d *demesne) unknownSlot(name string) *rpcError {
	if name == "" {
		return &rpcError{Code: rpcBadParams, Message: "params.slot is required", Data: d.slotNames()}
	}
	return &rpcError{Code: rpcBadParams, Message: "no such slot: " + name, Data: d.slotNames()}
}

func (d *demesne) slotNames() []string {
	out := make([]string, 0, len(d.slots))
	for _, s := range d.slots {
		out = append(out, s.specOf().name)
	}
	return out
}
