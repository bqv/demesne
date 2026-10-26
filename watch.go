// stadi watch: the LLM-side health policy.
//
// Why it lives in stadi: stadi sits on every request path, so it sees a dead or
// wedged llama router on the first request that fails -- earlier and with better
// evidence than any out-of-band probe, which has to wait for its own timer.
//
// The motivating incident (2026-10-26):
//
//	21:46:34 httputil: ReverseProxy read error during body copy: unexpected EOF
//	21:46:36 POST /v1/chat/completions -> 127.0.0.1:55555: connection refused
//	21:46:38..21:46:59 five more refusals, ~15 GiB of VRAM still allocated
//	(no restart; the card stayed hostage until the service was restarted at 22:16)
//
// The proxy knew 24 ms into the incident and only wrote it to a log. This file is
// the policy that turns that knowledge into an action.
//
// Deliberately traffic-driven: no tickers, no health probes, no allocation while
// idle. A breaker plus one bounded restart, and a refusal to restart in a loop.
//
// What it will NOT do: reset the GPU driver, unload or choose a model, or kill the
// sd child. If the router is gone and its VRAM never came back, the honest answer is
// "orphaned context, a restart cannot fix this" and a human decides -- a restart loop
// would only burn disk and hide the fault.

package main

import (
	"fmt"
	"log"
	"os/exec"
	"sync"
	"time"
)

// llmWatch is the whole state of the LLM side. One mutex, no timers.
type llmWatch struct {
	mu sync.Mutex

	fails     int       // consecutive failures since the last success
	openUntil time.Time // breaker: fail fast until this moment
	restarts  []time.Time
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

// recoverLLM is the only mutation in this file: restart the llama service, then check
// whether the card actually came back. It runs in its own goroutine so the failed
// request returns immediately instead of waiting out a model load.
func (b *broker) recoverLLM(reason string) {
	if !b.llm.budget(time.Now(), b.cfg.restartWindow, b.cfg.restartMax) {
		log.Printf("watch: llama failed (%s) but no restart is allowed now "+
			"(budget spent, or an earlier restart is still finishing) -- failing fast", reason)
		return
	}
	go func() {
		defer b.llm.restartDone()

		// Orphaned context: the engine is gone but the driver still holds its VRAM.
		// Seen at 21:46 today: port dead, ~15 GiB still allocated. Restarting into
		// that state cannot succeed -- llama would fail to allocate and we would be
		// restarting in a loop, so say what it is and stop.
		if !portAlive(b.cfg.llm) {
			if used, err := vramUsed(); err == nil && used > b.cfg.orphanAbove {
				detail := fmt.Sprintf("llama is not listening but %.2f GiB of VRAM is still allocated; "+
					"restarting cannot free an orphaned context", float64(used)/(1<<30))
				log.Printf("watch: %s", detail)
				b.llm.markOrphaned(detail)
				b.notify("stadi: " + detail)
				return
			}
		}

		log.Printf("watch: restarting %s after: %s", b.cfg.llmService, reason)
		if out, err := exec.Command("rc-service", "--user", b.cfg.llmService, "restart").CombinedOutput(); err != nil {
			detail := fmt.Sprintf("restart of %s failed: %v: %s", b.cfg.llmService, err, trim(string(out), 300))
			log.Printf("watch: %s", detail)
			b.llm.markOrphaned(detail) // no further attempts; a human decides
			b.notify("stadi: " + detail)
			return
		}
		if err := waitPort(b.cfg.llm, b.cfg.llmRestartWait); err != nil {
			detail := fmt.Sprintf("%s did not come back on %s within %s: %v",
				b.cfg.llmService, b.cfg.llm, b.cfg.llmRestartWait, err)
			log.Printf("watch: %s", detail)
			b.notify("stadi: " + detail)
			return
		}
		log.Printf("watch: %s is back on %s (%s)", b.cfg.llmService, b.cfg.llm, vramString())
	}()
}

// notify runs an optional notifier (STADI_NOTIFY_CMD), best effort. An alerting path
// that can break the broker would be worse than no alerting.
func (b *broker) notify(msg string) {
	if len(b.cfg.notifyCmd) == 0 {
		return
	}
	args := append(append([]string{}, b.cfg.notifyCmd[1:]...), msg)
	if err := exec.Command(b.cfg.notifyCmd[0], args...).Run(); err != nil {
		log.Printf("watch: notifier failed: %v", err)
	}
}

func trim(s string, n int) string {
	s = string([]rune(s))
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
