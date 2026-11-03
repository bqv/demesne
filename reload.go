// reload.go -- applying a changed conf.d to a running estate.
//
// The process environment is fixed at exec, so a running demesne cannot see an edited
// conf.d through os.Getenv: OpenRC sourced the file into a shell that exec'd the daemon,
// and only that shell ever saw the values. A reload therefore reads the file the way
// OpenRC does -- with a shell -- and then applies only what changed.
//
// What a reload may change: a child's binary, command line, environment, working directory,
// URL, idle clock and memory fence, plus whether demesne supervises it at all. What it may
// NOT change: where demesne listens, because that is the way in -- a reload that moved it
// would drop every client mid-stream. That is refused with a warning and the rest is still
// applied.
//
// A changed child is REPLACED, not mutated: an engine keeps the argv it was started with,
// so "new args" means a new process. A resident engine is stopped and its supervisor starts
// it again from the new spec; an on-demand one stays down until the next request. A
// reload that changes nothing costs nothing -- every child keeps running.

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"
)

// reloadReport is what a reload did, in the terms an operator reads.
type reloadReport struct {
	Restarted []string `json:"restarted"`
	Adopted   []string `json:"updated_in_place"`
	Refused   []string `json:"refused"`
	Unchanged []string `json:"unchanged"`
}

func (r reloadReport) String() string {
	part := func(label string, items []string) string {
		if len(items) == 0 {
			return ""
		}
		return fmt.Sprintf("%s %s; ", label, strings.Join(items, " "))
	}
	s := part("restarted:", r.Restarted) + part("in place:", r.Adopted) +
		part("refused:", r.Refused) + part("unchanged:", r.Unchanged)
	return strings.TrimSuffix(s, "; ")
}

// readConfFile asks a shell what the configuration file exports.
//
// The shell runs with a minimal environment on purpose. Inheriting demesne's own would mean
// a value deleted from the file survived the reload (because it was still in the process
// environment), so "reload" would quietly mean "reload what was already true" -- the exact
// trap this function exists to avoid.
func readConfFile(path string) (map[string]string, error) {
	if path == "" {
		return nil, errors.New("no configuration file to reload from: set -conf / DEMESNE_CONF")
	}
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("reload: %w", err)
	}
	home := os.Getenv("HOME")
	if home == "" {
		home = "/home/user"
	}
	cmd := exec.Command("/bin/sh", "-c", `. "$1" >/dev/null 2>&1; env`, "sh", path)
	cmd.Env = []string{"HOME=" + home, "PATH=/usr/local/bin:/usr/bin:/bin"}
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("reload: sourcing %s failed: %w", path, err)
	}
	values := make(map[string]string)
	for _, line := range strings.Split(string(out), "\n") {
		if k, v, ok := strings.Cut(line, "="); ok && k != "" {
			values[k] = v
		}
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("reload: %s exported nothing", path)
	}
	return values, nil
}

// enginesFrom builds the four engine specs and the two idle clocks from a key/value source.
// It is the reload's twin of loadConfig's per-engine block, and it deliberately applies no
// command-line flags: a flag was set once, at boot, and a file that does not mention it
// must not silently erase it. In practice the service passes no flags at all.
func enginesFrom(get func(string) string) (map[string]engineConfig, time.Duration, time.Duration, string) {
	// The URL key is not the slot name: llama's is DEMESNE_LLM. Everything else is the
	// slot uppercased.
	engine := func(urlKey, slot, defaultURL string) engineConfig {
		key := strings.ToUpper(slot)
		return engineConfig{
			url:  firstNonEmpty(get(urlKey), defaultURL),
			bin:  get(key + "_BIN"),
			args: fields(get(key + "_ARGS")),
			env:  fields(get(key + "_ENV")),
			dir:  get(key + "_DIR"),
		}
	}
	cgroupRoot := firstNonEmpty(get("CGROUP_ROOT"), cgroupRootDefault)
	return map[string]engineConfig{
		"llama":   engine("LLM", "llama", defaultLlamaURL),
		"llamb":   engine("LLAMB", "llamb", defaultLlambURL),
		"sd":      engine("SD", "sd", defaultSDURL),
		"whisper": engine("WHISPER", "whisper", defaultWhisperURL),
	}, envDurFrom(get, "IDLE", 30*time.Minute), envDurFrom(get, "WHISPER_IDLE", 10*time.Minute), cgroupRoot
}

func envDurFrom(get func(string) string, key string, def time.Duration) time.Duration {
	if v := get(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func firstNonEmpty(v, def string) string {
	if v != "" {
		return v
	}
	return def
}

// policyFor is the one place that decides how an engine is held: with a command line it is
// ours (resident for the routers, on demand for the media children), without one it is
// somebody else's.
func policyFor(name, bin string) policy {
	if bin == "" {
		return policyExternal
	}
	switch name {
	case "llama", "llamb":
		return policyResident
	}
	return policyOnDemand
}

// reload re-reads the configuration file and applies what changed. Everything it does not
// have to touch, it does not touch.
func (d *demesne) reload() (reloadReport, error) {
	var rep reloadReport
	values, err := readConfFile(d.cfg.confFile)
	if err != nil {
		return rep, err
	}
	get := envFromMap(values)
	engines, idle, whisperIdle, cgroupRoot := enginesFrom(get)

	if v := firstNonEmpty(get("LISTEN"), d.cfg.listen); v != d.cfg.listen {
		rep.Refused = append(rep.Refused, "listen ("+d.cfg.listen+" -> "+v+", needs a restart: it is the way in)")
	}
	if v := get("SOCKET"); v != d.cfg.socket {
		rep.Refused = append(rep.Refused, "socket (needs a restart: it is the way in)")
	}

	d.transition.Lock()
	defer d.transition.Unlock()

	names := make([]string, 0, len(d.slots))
	for _, s := range d.slots {
		names = append(names, s.specOf().name)
	}
	sort.Strings(names)

	for _, name := range names {
		s := d.slotByName(name)
		e := engines[name]
		cur := s.specOf()

		want := cur
		want.url, want.bin, want.args, want.env, want.dir = e.url, e.bin, e.args, e.env, e.dir
		want.policy = policyFor(name, e.bin)
		switch name {
		case "sd":
			want.idle = idle
		case "whisper":
			want.idle = whisperIdle
		}
		want.cgroup = memLimitFor(cgroupRoot, name, get)

		if specEqual(cur, want) {
			rep.Unchanged = append(rep.Unchanged, name)
			continue
		}

		// The child's own fields moved: it has to be a new process. Everything else can
		// be applied to a running one.
		childMoved := cur.bin != want.bin || !sameStrings(cur.args, want.args) ||
			!sameStrings(cur.env, want.env) || cur.dir != want.dir || cur.url != want.url

		up, _ := s.isUp()
		s.setSpec(want)

		if cur.url != want.url {
			d.proxies[name] = reverseProxy(d, want.url, name == "llama")
		}
		if cur.cgroup != want.cgroup && want.cgroup.set() {
			d.prepareCgroup(s)
		}
		if want.cgroup.set() && !cur.cgroup.set() {
			// A fence appeared: the running child is outside it.
			if up {
				childMoved = true
			}
		}
		if want.policy != policyResident && s.isSupervised() {
			s.setSupervised(false)
			if up {
				d.evictLocked(s, "demesne no longer supervises it")
			}
		}
		if want.policy == policyResident && !s.isSupervised() {
			s.setSupervised(true)
			go d.supervise(s)
		}
		if childMoved && up {
			d.evictLocked(s, "its configuration changed")
			rep.Restarted = append(rep.Restarted, name)
			continue
		}
		rep.Adopted = append(rep.Adopted, name)
	}
	return rep, nil
}

func specEqual(a, b slotSpec) bool {
	return a.url == b.url && a.bin == b.bin && sameStrings(a.args, b.args) &&
		sameStrings(a.env, b.env) && a.dir == b.dir && a.policy == b.policy &&
		a.idle == b.idle && a.cgroup == b.cgroup
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// handleReload is the same operation over the control plane, so a reload can be asked for
// from the socket, the CLI or a unit file without hunting for a pid to signal.
func (d *demesne) handleReload(w http.ResponseWriter, r *http.Request) {
	rep, err := d.reload()
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	log.Printf("reload: %s", rep.String())
	_ = json.NewEncoder(w).Encode(rep)
}
