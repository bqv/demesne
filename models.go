// models.go -- which engine serves which model.
//
// The estate used to be addressed by port: :55555 was the router, :55556 the aux router,
// and a client got whichever engine its number happened to be. That is why the numbers
// could not be reorganised without breaking every caller, and why a model living on the
// small card was reachable only by knowing a different port.
//
// demesne sits on the canonical number now, so the model NAME decides the engine and the
// path only decides the kind of request. Both routers answer /v1/models; the index below is
// the union of those answers, refreshed on a timer.
//
// A router that is down contributes nothing, so its models stop routing until it is back.
// That is the same answer a direct dial would give, only sooner and with a body.

package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	// modelIndexTTL is how stale the index may be. A ticker rather than refresh-on-miss:
	// a miss is a request, so refreshing in the caller makes one client pay the routers'
	// round-trip and the rest queue behind it.
	modelIndexTTL = 30 * time.Second
	// modelListTimeout bounds one router's /v1/models. Short on purpose: this runs on a
	// timer and must never outlast the tick.
	modelListTimeout = 2 * time.Second
	// modelBodyCap bounds how much of a request body is read to find the model name. A
	// chat body is kilobytes; anything past this is not a request worth classifying, so
	// it is forwarded unclassified rather than buffered.
	modelBodyCap = 8 << 20
)

// modelIndex is the union of the routers' advertised models. It is written only by
// refreshModels and read only by slotForModel, both under the mutex, so a refresh cannot
// tear a lookup.
type modelIndex struct {
	mu      sync.Mutex
	byModel map[string]string // model id -> slot name
	// loaded counts, per router, how many models it says are resident. It is what makes
	// "who holds the card" an answer about memory rather than about a process: a router
	// that is up with nothing loaded holds the port and about 28 MiB, not the card.
	loaded map[string]int
	// known is the set of routers that actually reported a status per model. A build that
	// does not would leave `loaded` at zero for an engine that may be holding gigabytes,
	// so its silence is treated as "cannot tell" rather than "empty".
	known map[string]bool
	seen  time.Time
}

func newModelIndex() modelIndex {
	return modelIndex{byModel: map[string]string{}, loaded: map[string]int{}, known: map[string]bool{}}
}

// refreshModels asks every router what it serves and swaps the map in one step.
func (d *demesne) refreshModels() {
	next := make(map[string]string)
	loaded := make(map[string]int)
	known := make(map[string]bool)
	for _, s := range d.slots {
		if !s.specOf().router || !s.specOf().routed {
			continue
		}
		items := fetchModels(s.specOf().url, modelListTimeout)
		if items != nil {
			n, saw := countLoaded(items)
			loaded[s.specOf().name], known[s.specOf().name] = n, saw
		}
		for _, id := range modelIDs(items) {
			// First router wins a clash. Two routers advertising one id is a
			// configuration mistake, and picking the earlier slot is at least stable.
			if _, taken := next[id]; !taken {
				next[id] = s.specOf().name
			}
		}
	}
	// A slot's configured models are routable even when the engine cannot answer: an on-demand
	// or external engine that is down has no /v1/models to read, so without this its names are
	// unknown after every restart and a request for one falls through to the router instead of
	// reaching the slot (or refusing). A live answer wins a clash, because it is the engine's
	// own word; the configured list is what the engine is known to serve.
	for _, s := range d.slots {
		if !s.specOf().router || !s.specOf().routed {
			continue
		}
		for _, id := range s.specOf().models {
			if _, taken := next[id]; !taken {
				next[id] = s.specOf().name
			}
		}
	}
	d.models.mu.Lock()
	changed := len(next) != len(d.models.byModel)
	d.models.byModel, d.models.loaded, d.models.known = next, loaded, known
	d.models.seen = time.Now()
	d.models.mu.Unlock()
	if changed {
		d.logModelIndex(next)
	}
}

func (d *demesne) logModelIndex(byModel map[string]string) {
	if len(byModel) == 0 {
		log.Printf("routing by model: no router is advertising models; unknown models go to the default slot")
		return
	}
	per := map[string][]string{}
	for id, slot := range byModel {
		per[slot] = append(per[slot], id)
	}
	parts := make([]string, 0, len(d.slots))
	for _, s := range d.slots {
		if ids := per[s.specOf().name]; len(ids) > 0 {
			parts = append(parts, s.specOf().name+"("+strings.Join(ids, " ")+")")
		}
	}
	log.Printf("routing by model: %s", strings.Join(parts, " "))
}

// slotForModel answers "which engine has this model", and "" when nobody does -- in which
// case the caller keeps the slot the path chose, i.e. the router, which is where an unknown
// model went before any of this existed.
func (d *demesne) slotForModel(model string) string {
	if model == "" {
		return ""
	}
	d.models.mu.Lock()
	defer d.models.mu.Unlock()
	return d.models.byModel[model]
}

// peeked is what a chat request says about itself before it is forwarded: which model, and
// whether it wants a stream. Both decide behaviour, and the body is kept so the peek costs
// the request nothing.
type peeked struct {
	body   []byte
	model  string
	stream bool
}

// peekRequest reads just enough of a request body to learn the model name and whether it
// streams, then hands the body back untouched: the proxy streams from the same bytes and the
// upstream never learns that anything looked.
func peekRequest(r *http.Request) peeked {
	if r.Body == nil || r.ContentLength > modelBodyCap {
		return peeked{}
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, modelBodyCap+1))
	r.Body = io.NopCloser(bytes.NewReader(body))
	if err != nil || len(body) > modelBodyCap {
		return peeked{}
	}
	var probe struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
	}
	if json.Unmarshal(body, &probe) != nil {
		return peeked{body: body}
	}
	return peeked{body: body, model: probe.Model, stream: probe.Stream}
}

// fetchModels returns a router's raw model list, kept as raw bytes so every entry travels
// with whatever fields its own engine publishes.
func fetchModels(base string, timeout time.Duration) []json.RawMessage {
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(strings.TrimRight(base, "/") + "/v1/models")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	var doc struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&doc); err != nil {
		return nil
	}
	return doc.Data
}

// countLoaded answers "how many of this router's models does it say are resident", and
// whether it said so at all. A router that publishes no status is not assumed empty: it is
// assumed unknown, and the caller falls back to the coarser process-level answer.
func countLoaded(items []json.RawMessage) (int, bool) {
	n, saw := 0, false
	for _, raw := range items {
		var one struct {
			Status struct {
				Value string `json:"value"`
			} `json:"status"`
		}
		if json.Unmarshal(raw, &one) != nil {
			continue
		}
		switch one.Status.Value {
		case "loaded", "loading":
			saw, n = true, n+1
		case "unloaded":
			saw = true
		}
	}
	return n, saw
}

// loadedCount is how many models a router has resident, and whether that is known.
func (d *demesne) loadedCount(slot string) (int, bool) {
	d.models.mu.Lock()
	defer d.models.mu.Unlock()
	n, known := d.models.loaded[slot], d.models.known[slot]
	return n, known
}

func modelIDs(items []json.RawMessage) []string {
	ids := make([]string, 0, len(items))
	for _, raw := range items {
		var one struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(raw, &one) == nil && one.ID != "" {
			ids = append(ids, one.ID)
		}
	}
	return ids
}

// followModels keeps the index fresh until demesne stops. The first refresh is done by the
// caller, before the listener opens, so the first request already routes by name.
func (d *demesne) followModels() {
	t := time.NewTicker(modelIndexTTL)
	defer t.Stop()
	for {
		select {
		case <-d.done:
			return
		case <-t.C:
			d.refreshModels()
		}
	}
}

// modelsHandler answers /v1/models for the whole estate. A client that dials the one port
// should see every model it can ask for, not only the ones the engine behind that path
// happens to know about.
func (d *demesne) modelsHandler(w http.ResponseWriter, r *http.Request) {
	data := []json.RawMessage{}
	answered := 0
	for _, s := range d.slots {
		if !s.specOf().router || !s.specOf().routed {
			continue
		}
		items := fetchModels(s.specOf().url, modelListTimeout)
		if items == nil {
			continue
		}
		answered++
		data = append(data, items...)
	}
	if answered == 0 {
		// Nobody answered. Let the default engine produce its own error instead of
		// inventing an empty list: "no models" and "the router is down" are not the same
		// answer, and the second one is the true one.
		if s := d.slotByName("llama"); s != nil {
			d.proxies[s.specOf().name].ServeHTTP(w, r)
			return
		}
		http.Error(w, "demesne: no router answered /v1/models", http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data})
}
