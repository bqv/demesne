package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	eventRingLen    = 64
	eventBuffer     = 16
	eventHeartbeat  = 15 * time.Second
	eventSnapshotAt = 2 * time.Second
)

type eventFrame struct {
	Seq        int64             `json:"seq"`
	Type       string            `json:"type"`
	AtMS       int64             `json:"at_ms"`
	Snapshot   *metricsDoc       `json:"snapshot,omitempty"`
	Load       *modelLoad        `json:"load,omitempty"`
	Transition *transitionRecord `json:"transition,omitempty"`
}

type eventHub struct {
	mu     sync.Mutex
	seq    int64
	ring   []eventFrame
	subs   map[int]chan eventFrame
	nextID int
	closed bool
}

func newEventHub() *eventHub {
	return &eventHub{ring: make([]eventFrame, 0, eventRingLen), subs: map[int]chan eventFrame{}}
}

func (h *eventHub) cursor() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.seq
}

func (h *eventHub) subscriberCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

func (h *eventHub) publish(f eventFrame) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.seq++
	f.Seq = h.seq
	if f.AtMS == 0 {
		f.AtMS = time.Now().UnixMilli()
	}
	h.ring = append(h.ring, f)
	if len(h.ring) > eventRingLen {
		h.ring = h.ring[len(h.ring)-eventRingLen:]
	}
	for id, ch := range h.subs {
		select {
		case ch <- f:
		default:
			_ = id
		}
	}
	h.mu.Unlock()
}

func (h *eventHub) subscribe() (int, chan eventFrame) {
	h.mu.Lock()
	defer h.mu.Unlock()
	id := h.nextID
	h.nextID++
	ch := make(chan eventFrame, eventBuffer)
	h.subs[id] = ch
	return id, ch
}

func (h *eventHub) unsubscribe(id int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if ch, ok := h.subs[id]; ok {
		delete(h.subs, id)
		close(ch)
	}
}

func (h *eventHub) since(seq int64) ([]eventFrame, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.ring) == 0 {
		return nil, seq == 0
	}
	if seq > h.seq {
		return nil, false
	}
	oldest := h.ring[0].Seq
	if seq+1 < oldest {
		return nil, false
	}
	out := make([]eventFrame, 0, len(h.ring))
	for _, f := range h.ring {
		if f.Seq > seq {
			out = append(out, f)
		}
	}
	return out, true
}

func (h *eventHub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	for id, ch := range h.subs {
		delete(h.subs, id)
		close(ch)
	}
}

func (d *demesne) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "demesne: GET required", http.StatusMethodNotAllowed)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "demesne: streaming is not available on this transport", http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	h.Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	write := func(f eventFrame) bool {
		b, err := json.Marshal(f)
		if err != nil {
			return false
		}
		if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", f.Seq, f.Type, b); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}

	id, ch := d.hub.subscribe()
	defer d.hub.unsubscribe(id)

	var cursor int64
	if v := r.Header.Get("Last-Event-ID"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			cursor = n
		}
	}
	if cursor > 0 {
		if replayed, ok := d.hub.since(cursor); ok {
			for _, f := range replayed {
				if !write(f) {
					return
				}
			}
		} else {
			cursor = 0
		}
	}
	if cursor == 0 {
		doc := d.metrics()
		first := eventFrame{Seq: d.hub.cursor(), Type: "snapshot", AtMS: time.Now().UnixMilli(), Snapshot: &doc}
		if !write(first) {
			return
		}
	}

	beat := time.NewTicker(eventHeartbeat)
	defer beat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-d.done:
			return
		case f, ok := <-ch:
			if !ok {
				return
			}
			if !write(f) {
				return
			}
		case <-beat.C:
			if _, err := fmt.Fprint(w, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (d *demesne) publishSnapshots() {
	t := time.NewTicker(eventSnapshotAt)
	defer t.Stop()
	for {
		select {
		case <-d.done:
			d.hub.close()
			return
		case <-t.C:
			if d.hub.subscriberCount() == 0 {
				continue
			}
			doc := d.metrics()
			d.hub.publish(eventFrame{Type: "snapshot", Snapshot: &doc})
		}
	}
}
