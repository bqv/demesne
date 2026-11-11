package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
)

// tenantActivity is what a ready tenant is doing this second, read from its own /slots. Prefill and
// KV are the only two the engine actually reports: this build answers no n_past and no n_decoded,
// so generation progress and tokens/second are not published rather than inferred.
type tenantActivity struct {
	IsProcessing    bool    `json:"is_processing"`
	PrefillPct      float64 `json:"prefill_pct,omitempty"`
	PromptTokens    int64   `json:"prompt_tokens,omitempty"`
	PromptProcessed int64   `json:"prompt_processed,omitempty"`
	KVPct           float64 `json:"kv_pct,omitempty"`
	SinceMS         int64   `json:"since_ms,omitempty"`
}

type slotReply struct {
	NCtx                   int64 `json:"n_ctx"`
	IsProcessing           bool  `json:"is_processing"`
	NPromptTokens          int64 `json:"n_prompt_tokens"`
	NPromptTokensProcessed int64 `json:"n_prompt_tokens_processed"`
	NPromptTokensCache     int64 `json:"n_prompt_tokens_cache"`
}

func (d *demesne) slotsClient(s *tenant) (*http.Client, string) {
	base := strings.TrimRight(s.specOf().url, "/")
	if path := unixPath(base); path != "" {
		return &http.Client{Transport: unixDialer(path)}, "http://unix-socket"
	}
	return http.DefaultClient, base
}

// pollActivity asks every ready tenant what it is doing. One request per second per ready tenant,
// which is cheaper than the /v1/models polling the router path used to do and tells a meter what a
// load board cannot.
func (d *demesne) pollActivity() {
	for _, s := range d.tenants {
		up, _ := s.isUp()
		if !up || s.booting() {
			continue
		}
		client, base := d.slotsClient(s)
		req, err := http.NewRequest(http.MethodGet, base+"/slots", nil)
		if err != nil {
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			continue
		}
		var slots []slotReply
		if json.Unmarshal(body, &slots) != nil || len(slots) == 0 {
			continue
		}
		slot := slots[0]
		for _, c := range slots {
			if c.IsProcessing {
				slot = c
				break
			}
		}
		a := &tenantActivity{
			IsProcessing: slot.IsProcessing, PromptTokens: slot.NPromptTokens,
			PromptProcessed: slot.NPromptTokensProcessed,
			SinceMS:         time.Now().UnixMilli(),
		}
		if slot.NPromptTokens > 0 {
			a.PrefillPct = float64(slot.NPromptTokensProcessed) / float64(slot.NPromptTokens) * 100
		}
		if slot.NCtx > 0 {
			used := slot.NPromptTokensCache + slot.NPromptTokensProcessed
			a.KVPct = float64(used) / float64(slot.NCtx) * 100
		}
		s.mu.Lock()
		s.activity = a
		s.mu.Unlock()
	}
}

func (d *demesne) followActivity() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-d.done:
			return
		case <-t.C:
			d.pollActivity()
		}
	}
}
