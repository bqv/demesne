// demesne watch: the LLM-side health policy.
//
// Why it lives here: demesne sits on every request path, so it sees a dead or wedged
// llama router on the first request that fails -- earlier and with better evidence than
// any out-of-band probe, which has to wait for its own timer.
//
// The motivating incident (2026-10-26):
//
//	21:46:34 httputil: ReverseProxy read error during body copy: unexpected EOF
//	21:46:36 POST /v1/chat/completions -> 127.0.0.1:55555: connection refused
//	21:46:38..21:46:59 five more refusals, ~15 GiB of VRAM still allocated
//	(no restart; the card stayed hostage until the service was restarted at 22:16)
//
// The proxy knew 24 ms into the incident and only wrote it to a log. This file is the
// policy that turns that knowledge into an action.
//
// Deliberately traffic-driven: no tickers, no health probes, no allocation while idle.
// A breaker plus one bounded restart, and a refusal to restart in a loop.
//
// What it will NOT do: reset the GPU driver, unload or choose a model, or kill the sd
// child. If the router is gone and its VRAM never came back, the honest answer is
// "orphaned context, a restart cannot fix this" and a human decides -- a restart loop
// would only burn disk and hide the fault.
//
// TWO ACTORS, ONE EVENT (fixed in the fork). This policy used to ask `rc-service --user
// llama restart` for a router that supervise-daemon was already respawning every 5 s.
// When demesne owns the router there is one actor: the supervisor, which restarts on
// exit by itself. A death here is therefore a KILL of a wedged child, not a start, and
// the supervisor does the rest. Only an engine left to OpenRC still takes the old path.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// llmWatch is the whole state of the LLM side. One mutex, no timers.
type llmWatch struct {
	mu sync.Mutex

	fails      int       // consecutive failures since the last success
	openUntil  time.Time // breaker: fail fast until this moment
	restarts   []time.Time
	restarting bool // a restart goroutine is in flight
	degraded   bool // budget spent; stop restarting, keep failing fast politely
	orphaned   bool // router down but its VRAM was never released
	lastErr    string
}

func newLLMWatch() *llmWatch { return &llmWatch{} }

// failing answers "may this request be dialled at all?" and, when not, how long the
// caller should wait. Degraded state is a breaker with a long TTL that a single
// success clears -- so the box recovers by itself once someone fixes the engine,
// without a restart storm in the meantime.
func (w *llmWatch) failing() (bool, time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if now := time.Now(); now.Before(w.openUntil) {
		return true, w.openUntil.Sub(now).Round(time.Second)
	}
	return false, 0
}

// fail records one proxy-observed failure and decides whether this is the moment to
// restart. The caller does the restarting, so nothing here blocks a request.
func (w *llmWatch) fail(err error, threshold int, breaker time.Duration) (restart bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.fails++
	w.lastErr = err.Error()
	if w.fails >= threshold {
		w.openUntil = time.Now().Add(breaker)
	}
	if w.restarting || w.degraded || w.orphaned {
		return false
	}
	return w.fails >= threshold
}

// ok is the recovery edge: one real answer from the engine clears everything. A
// successful stream is worth more than a health probe, which is why success comes
// from the proxy rather than a timer.
func (w *llmWatch) ok() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fails > 0 {
		log.Printf("watch: llama is answering again after %d failure(s)", w.fails)
	}
	w.fails = 0
	w.openUntil = time.Time{}
	w.degraded = false
	w.orphaned = false
}

// budget reports whether one more restart is allowed, and claims the slot so two
// failing requests cannot start two restarts.
func (w *llmWatch) budget(now time.Time, window time.Duration, max int) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.restarting || w.degraded || w.orphaned {
		return false
	}
	kept := w.restarts[:0]
	for _, t := range w.restarts {
		if now.Sub(t) < window {
			kept = append(kept, t)
		}
	}
	w.restarts = kept
	if len(kept) >= max {
		w.degraded = true
		return false
	}
	w.restarting = true
	w.restarts = append(w.restarts, now)
	return true
}

// restarted clears the breaker once the service is listening again. The restart is the
// strongest evidence available without a request, and leaving the breaker to run out
// its window would keep answering 503 for the rest of it after a real recovery.
func (w *llmWatch) restarted() {
	w.mu.Lock()
	w.fails = 0
	w.openUntil = time.Time{}
	w.mu.Unlock()
}

func (w *llmWatch) restartDone() {
	w.mu.Lock()
	w.restarting = false
	w.mu.Unlock()
}

func (w *llmWatch) markOrphaned(detail string) {
	w.mu.Lock()
	w.orphaned = true
	w.degraded = true
	w.lastErr = detail
	w.openUntil = time.Now().Add(24 * time.Hour)
	w.mu.Unlock()
}

// snapshot is what /status publishes: enough to tell "broken" from "healing".
func (w *llmWatch) snapshot() map[string]any {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := map[string]any{
		"fails":      w.fails,
		"restarts":   len(w.restarts),
		"degraded":   w.degraded,
		"orphaned":   w.orphaned,
		"restarting": w.restarting,
	}
	if w.lastErr != "" {
		s["last_error"] = w.lastErr
	}
	if now := time.Now(); now.Before(w.openUntil) {
		s["open_for_s"] = w.openUntil.Sub(now).Round(time.Second).Seconds()
	}
	return s
}

// ------------------------------------------------------------- the router, itself

// unloadLLM asks the router to drop every model it has resident. The router's unload is
// async and force-kills a loading child, so its own answer is the authority. That is
// what makes a resident router compatible with a media child: one gives the card back
// by unloading, the other only by exiting.
func (d *demesne) unloadLLM() {
	ids, err := routerModels(d.cfg.llama.url)
	if err != nil {
		log.Printf("warning: cannot list router models: %v", err)
		return
	}
	if len(ids) == 0 {
		return
	}
	for _, id := range ids {
		body := strings.NewReader(`{"model":` + strconv.Quote(id) + `}`)
		req, err := http.NewRequest(http.MethodPost, d.cfg.llama.url+"/models/unload", body)
		if err != nil {
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Printf("unload %s: %v", id, err)
			continue
		}
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		log.Printf("unload %s -> %s %s", id, resp.Status, trim(string(msg), 200))
	}
}

func routerModels(base string) ([]string, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(base + "/v1/models")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var payload struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&payload); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(payload.Data))
	for _, m := range payload.Data {
		ids = append(ids, m.ID)
	}
	return ids, nil
}

// recoverLLM is the only mutation in this file. It runs in its own goroutine so the
// failed request returns immediately instead of waiting out a model load.
func (d *demesne) recoverLLM(reason string) {
	if !d.llm.budget(time.Now(), d.cfg.restartWindow, d.cfg.restartMax) {
		log.Printf("watch: llama failed (%s) but no restart is allowed now "+
			"(budget spent, or an earlier restart is still finishing) -- failing fast", reason)
		return
	}
	go func() {
		defer d.llm.restartDone()

		// Orphaned context: the engine is gone but the driver still holds its VRAM.
		// Seen at 21:46: port dead, ~15 GiB still allocated. Restarting into that state
		// cannot succeed -- llama would fail to allocate and we would be restarting in a
		// loop, so say what it is and stop.
		if !portAlive(d.cfg.llama.url) {
			if used, err := vramUsed(); err == nil && used > d.cfg.orphanAbove {
				detail := fmt.Sprintf("llama is not listening but %.2f GiB of VRAM is still allocated; "+
					"restarting cannot free an orphaned context", float64(used)/(1<<30))
				log.Printf("watch: %s", detail)
				d.llm.markOrphaned(detail)
				return
			}
		}

		if s := d.slotByName("llama"); s != nil && s.specOf().own() {
			// demesne owns the router: the supervisor is the only actor that starts it,
			// so the useful thing here is to take down a child that is not answering.
			// If it is already gone, this is a no-op and the supervisor is already
			// dealing with it -- which is the double restart, removed.
			log.Printf("watch: restarting the llama child after: %s", reason)
			d.evict(s, "the watch decided the router is wedged")
			return
		}

		log.Printf("watch: restarting %s after: %s", d.cfg.llmService, reason)
		if out, err := exec.Command(d.cfg.rcService, "--user", d.cfg.llmService, "restart").CombinedOutput(); err != nil {
			detail := fmt.Sprintf("restart of %s failed: %v: %s", d.cfg.llmService, err, trim(string(out), 300))
			log.Printf("watch: %s", detail)
			d.llm.markOrphaned(detail) // no further attempts: a human decides
			return
		}
		if err := waitPort(d.cfg.llama.url, d.cfg.llmRestartWait, d.done); err != nil {
			detail := fmt.Sprintf("%s did not come back on %s within %s: %v",
				d.cfg.llmService, d.cfg.llama.url, d.cfg.llmRestartWait, err)
			log.Printf("watch: %s", detail)
			return
		}
		d.llm.restarted()
		log.Printf("watch: %s is back on %s (%s)", d.cfg.llmService, d.cfg.llama.url, vramString())
	}()
}

func trim(s string, n int) string {
	s = string([]rune(s))
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
