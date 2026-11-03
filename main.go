// demesne -- the part of a manor the lord keeps in his own hand: the resident set.
//
// demesne is the single owner of the local-inference estate on this box. Four engines
// run here and three of them contend for something scarce:
//
//	llama   :55555  the llama.cpp router. Keeps a resident rung: 17.6-19.7 GiB of 19.98.
//	sd      :55557  stable-diffusion.cpp. A DiT wants ~12.5 GiB and it CANNOT unload --
//	                there is no flag and no endpoint, only the process exiting frees
//	                the weights (measured 12.5 GiB -> 0.03 GiB).
//	llamb   :55556  the aux router on the second card (GT 1030). Supervised, not routed:
//	                its clients address it directly.
//	whisper :55559  whisper.cpp, CPU-only. It used to claim 55557 as well -- the port
//	                story slot.go settles.
//
// The rules are in slot.go; this file is configuration, routing, /status and the
// operator's CLI.
//
// stadi's duties survive unchanged: the proxy in front of the router, the LLM breaker,
// idle eviction of the media child, /status. What changes is that demesne fork/execs
// the engines it owns instead of asking rc-service to do it, so there is ONE actor per
// restart -- the double restart (supervise-daemon respawning llama every 5 s while
// recoverLLM asked rc-service for the same thing) cannot happen. An engine can still be
// left to OpenRC: give it no command line and it stays policyExternal, routed and never
// touched.
//
// No third-party code: stdlib only, so it builds with no module cache and no network.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const version = "0.3.0" // 0.2.0 was stadi at ad49321; the fork line starts here

// The environment keeps stadi's names working, because conf.d/stadi already exports
// them and a rename that breaks the deployed service at boot is not a rename, it is an
// outage. DEMESNE_* wins; STADI_* is read with a note.
const (
	envPrefix    = "DEMESNE_"
	legacyPrefix = "STADI_"
)

// ---------------------------------------------------------------- configuration

// engineConfig is one engine's static configuration: where it answers, what to run,
// and the memory fence that used to live in its init script.
type engineConfig struct {
	url    string
	bin    string
	args   []string
	env    []string
	cgroup memLimit
}

type config struct {
	listen                    string // where demesne itself listens (the port DSH uses)
	socket                    string // the same API on a unix socket; empty = no socket
	sd, llama, llamb, whisper engineConfig
	idle                      time.Duration // evict an on-demand child after this
	whisperIdle               time.Duration
	busyWait                  time.Duration // a chat request waits this long for a render
	vramWait                  time.Duration // bound on waiting for the card to return
	loadWait                  time.Duration // bound on a child's port appearing
	freeBelow                 uint64        // bytes: "the card is free" threshold
	orphanAbove               uint64        // bytes: VRAM still held while the port is dead
	logDir                    string
	cgroupRoot                string
	cgroupPrepare             string // cgroup-prepare helper (root, via sudo -n)
	cgroupJoin                string
	restartDelay              time.Duration // first delay between resident restarts

	// LLM watch (watch.go). Traffic-driven: no timer, no health probe.
	llmService     string        // legacy: the OpenRC service that owns the router
	llmRestartWait time.Duration // how long a legacy restart may take to become dialable
	restartWindow  time.Duration
	restartMax     int
	breakerFor     time.Duration
	failThreshold  int
	rcService      string // absolute path to rc-service (the legacy restarter)
}

// envString resolves a setting: the flag when given, else DEMESNE_<key>, else
// STADI_<key> (with a note), else the default. Flags win, but an existing conf.d keeps
// working unchanged, so the service needs no flag plumbing to stay correct.
func envString(flagVal, key, def string) string {
	if flagVal != "" {
		return flagVal
	}
	if v := os.Getenv(envPrefix + key); v != "" {
		return v
	}
	if v := os.Getenv(legacyPrefix + key); v != "" {
		log.Printf("note: %s%s is the old name for %s%s; both are read for now",
			legacyPrefix, key, envPrefix, key)
		return v
	}
	return def
}

func envDur(flagVal, key string, def time.Duration) time.Duration {
	s := envString(flagVal, key, "")
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		log.Printf("bad duration %q (flag or %s%s), using %s", s, envPrefix, key, def)
		return def
	}
	return d
}

// envInt is envDur's integer sibling: a non-positive result never silently disables a
// safety rule.
func envInt(flagVal int, key string, def int) int {
	if flagVal > 0 {
		return flagVal
	}
	for _, name := range []string{envPrefix + key, legacyPrefix + key} {
		v := os.Getenv(name)
		if v == "" {
			continue
		}
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n > 0 {
			return n
		}
		log.Printf("warning: %s=%q is not a positive integer; using %d", name, v, def)
		return def
	}
	return def
}

func fields(s string) []string { return strings.Fields(s) }

// memLimitFor reads a slot's memory fence from the environment. Env-only on purpose:
// these are set once in conf.d/demesne, never typed on a command line, and there are
// three of them per slot.
func memLimitFor(root, slot string) memLimit {
	m := memLimit{
		leaf:     filepath.Join(root, slot),
		high:     envRaw(slot + "_MEMORY_HIGH"),
		max:      envRaw(slot + "_MEMORY_MAX"),
		swapHigh: envRaw(slot + "_SWAP_HIGH"),
	}
	if m.high == "" && m.max == "" && m.swapHigh == "" {
		return memLimit{}
	}
	return m
}

func envRaw(key string) string {
	if v := os.Getenv(envPrefix + key); v != "" {
		return v
	}
	return os.Getenv(legacyPrefix + strings.ToUpper(key))
}

// defaultLogDir follows the XDG cache location rather than assuming a home path.
func defaultLogDir() string {
	if d, err := os.UserCacheDir(); err == nil {
		return filepath.Join(d, "demesne")
	}
	return filepath.Join(os.TempDir(), "demesne")
}

// defaultBin finds a binary on PATH instead of assuming /usr/bin. Empty when absent:
// for the engines demesne may adopt, "not installed" must mean "not adopted", never
// "adopted a name that does not resolve".
func defaultBin(name string) string {
	if p, err := exec.LookPath(name); err == nil {
		return p
	}
	return ""
}

// defaultRCService resolves the OpenRC client. The warning is for a reason: a service
// started by supervise-daemon may have a PATH without /sbin, and a restarter that
// cannot be found is exactly the failure this file exists to remove.
func defaultRCService() string {
	if p, err := exec.LookPath("rc-service"); err == nil {
		return p
	}
	for _, p := range []string{"/sbin/rc-service", "/usr/sbin/rc-service", "/bin/rc-service"} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	log.Printf("warning: rc-service was not found; the legacy restart action will fail")
	return "rc-service"
}

func loadConfig() config {
	var (
		fListen  = flag.String("listen", "", "address to listen on (env "+envPrefix+"LISTEN)")
		fSocket  = flag.String("socket", "", "serve the same API on this unix socket too (env "+envPrefix+"SOCKET)")
		fLLM     = flag.String("llm", "", "the llama router's base URL (env "+envPrefix+"LLM)")
		fSD      = flag.String("sd", "", "URL the sd child comes up on (env "+envPrefix+"SD)")
		fLlamb   = flag.String("llamb", "", "the auxiliary router's base URL (env "+envPrefix+"LLAMB)")
		fWhisper = flag.String("whisper", "", "URL the whisper child comes up on (env "+envPrefix+"WHISPER)")

		fIdle        = flag.String("idle", "", "evict an on-demand child after this long with no request (env "+envPrefix+"IDLE)")
		fWhisperIdle = flag.String("whisper-idle", "", "the same for whisper (env "+envPrefix+"WHISPER_IDLE)")
		fBusyWait    = flag.String("busy-wait", "", "how long a chat request waits for a render (env "+envPrefix+"BUSY_WAIT)")
		fVramWait    = flag.String("vram-wait", "", "bound on waiting for the card to come back (env "+envPrefix+"VRAM_WAIT)")
		fLoadWait    = flag.String("load-wait", "", "bound on a child's port appearing (env "+envPrefix+"LOAD_WAIT)")
		fFreeGiB     = flag.Float64("free-gib", 2, "treat the card as free below this many GiB used")
		fRestartWait = flag.String("restart-delay", "", "first delay between resident restarts (env "+envPrefix+"RESTART_DELAY)")

		fLogDir    = flag.String("log-dir", "", "directory for demesne's and the children's logs (env "+envPrefix+"LOG_DIR)")
		fSDLogDir  = flag.String("sd-log-dir", "", "deprecated: use -log-dir")
		fCgroup    = flag.String("cgroup-root", "", "delegated cgroup v2 subtree for the slots (env "+envPrefix+"CGROUP_ROOT)")
		fCgPrepare = flag.String("cgroup-prepare", "", "cgroup-prepare helper, run via sudo -n (env "+envPrefix+"CGROUP_PREPARE)")
		fCgJoin    = flag.String("cgroup-join", "", "cgroup-join helper, run via sudo -n (env "+envPrefix+"CGROUP_JOIN)")

		fSDBin  = flag.String("sd-bin", "", "sd-server binary (env "+envPrefix+"SD_BIN; default: PATH)")
		fSDArgs = flag.String("sd-args", "", "the sd child's whole command line (env "+envPrefix+"SD_ARGS)")
		fSDEnv  = flag.String("sd-env", "", "extra KEY=VALUE for the sd child (env "+envPrefix+"SD_ENV)")

		fLlamaBin  = flag.String("llama-bin", "", "adopt the llama router: its binary (env "+envPrefix+"LLAMA_BIN; empty = leave it to OpenRC)")
		fLlamaArgs = flag.String("llama-args", "", "the router's whole command line (env "+envPrefix+"LLAMA_ARGS)")
		fLlamaEnv  = flag.String("llama-env", "", "extra KEY=VALUE for the router (env "+envPrefix+"LLAMA_ENV)")

		fLlambBin  = flag.String("llamb-bin", "", "adopt the aux router: its binary (env "+envPrefix+"LLAMB_BIN; empty = leave it to OpenRC)")
		fLlambArgs = flag.String("llamb-args", "", "the aux router's whole command line (env "+envPrefix+"LLAMB_ARGS)")
		fLlambEnv  = flag.String("llamb-env", "", "extra KEY=VALUE for the aux router (env "+envPrefix+"LLAMB_ENV)")

		fWhisperBin  = flag.String("whisper-bin", "", "adopt whisper: its binary (env "+envPrefix+"WHISPER_BIN)")
		fWhisperArgs = flag.String("whisper-args", "", "whisper's whole command line (env "+envPrefix+"WHISPER_ARGS)")
		fWhisperEnv  = flag.String("whisper-env", "", "extra KEY=VALUE for whisper (env "+envPrefix+"WHISPER_ENV)")

		fLLMSvc  = flag.String("llm-service", "", "legacy: the OpenRC service that owns the router (env "+envPrefix+"LLM_SERVICE)")
		fLLMWait = flag.String("llm-restart-wait", "", "legacy: how long a router restart may take to answer (env "+envPrefix+"LLM_RESTART_WAIT)")
		fWin     = flag.String("restart-window", "", "window for the restart budget (env "+envPrefix+"RESTART_WINDOW)")
		fMax     = flag.Int("restart-max", 0, "restarts allowed inside the window (env "+envPrefix+"RESTART_MAX)")
		fBreaker = flag.String("breaker", "", "fail fast for this long after a failure burst (env "+envPrefix+"BREAKER)")
		fFails   = flag.Int("fail-threshold", 0, "consecutive failures that trip the breaker (env "+envPrefix+"FAIL_THRESHOLD)")
		fRCSvc   = flag.String("rc-service", "", "path to rc-service, used by the legacy restarter (env "+envPrefix+"RC_SERVICE)")
		fVersion = flag.Bool("version", false, "print the version and exit")
	)
	flag.Parse()

	if *fVersion {
		fmt.Println("demesne " + version)
		os.Exit(0)
	}

	logDir := envString(*fLogDir, "LOG_DIR", "")
	if logDir == "" {
		// conf.d/stadi passes --sd-log-dir; the name is now too narrow, but breaking
		// a deployed flag set to win an argument about spelling is a bad trade.
		logDir = envString(*fSDLogDir, "SD_LOG_DIR", "")
	}
	if logDir == "" {
		logDir = defaultLogDir()
	}

	freeGiB := *fFreeGiB
	if freeGiB <= 0 {
		freeGiB = 2
	}
	cgroupRoot := envString(*fCgroup, "CGROUP_ROOT", "/sys/fs/cgroup/demesne")

	return config{
		listen: envString(*fListen, "LISTEN", "127.0.0.1:55558"),
		socket: envString(*fSocket, "SOCKET", ""),
		sd: engineConfig{
			url:    envString(*fSD, "SD", "http://127.0.0.1:55557"),
			bin:    envString(*fSDBin, "SD_BIN", defaultBin("sd-server")),
			args:   fields(envString(*fSDArgs, "SD_ARGS", "")),
			env:    fields(envString(*fSDEnv, "SD_ENV", "")),
			cgroup: memLimitFor(cgroupRoot, "sd"),
		},
		llama: engineConfig{
			url:    envString(*fLLM, "LLM", "http://127.0.0.1:55555"),
			bin:    envString(*fLlamaBin, "LLAMA_BIN", ""),
			args:   fields(envString(*fLlamaArgs, "LLAMA_ARGS", "")),
			env:    fields(envString(*fLlamaEnv, "LLAMA_ENV", "")),
			cgroup: memLimitFor(cgroupRoot, "llama"),
		},
		llamb: engineConfig{
			url:    envString(*fLlamb, "LLAMB", "http://127.0.0.1:55556"),
			bin:    envString(*fLlambBin, "LLAMB_BIN", ""),
			args:   fields(envString(*fLlambArgs, "LLAMB_ARGS", "")),
			env:    fields(envString(*fLlambEnv, "LLAMB_ENV", "")),
			cgroup: memLimitFor(cgroupRoot, "llamb"),
		},
		whisper: engineConfig{
			url:    envString(*fWhisper, "WHISPER", "http://127.0.0.1:55559"),
			bin:    envString(*fWhisperBin, "WHISPER_BIN", ""),
			args:   fields(envString(*fWhisperArgs, "WHISPER_ARGS", "")),
			env:    fields(envString(*fWhisperEnv, "WHISPER_ENV", "")),
			cgroup: memLimitFor(cgroupRoot, "whisper"),
		},
		idle:          envDur(*fIdle, "IDLE", 5*time.Minute),
		whisperIdle:   envDur(*fWhisperIdle, "WHISPER_IDLE", 10*time.Minute),
		busyWait:      envDur(*fBusyWait, "BUSY_WAIT", 2*time.Minute),
		vramWait:      envDur(*fVramWait, "VRAM_WAIT", 45*time.Second),
		loadWait:      envDur(*fLoadWait, "LOAD_WAIT", 15*time.Minute),
		freeBelow:     uint64(freeGiB * float64(1<<30)),
		orphanAbove:   uint64(1 << 30),
		logDir:        logDir,
		cgroupRoot:    cgroupRoot,
		cgroupPrepare: envString(*fCgPrepare, "CGROUP_PREPARE", defaultBin("cgroup-prepare")),
		cgroupJoin:    envString(*fCgJoin, "CGROUP_JOIN", defaultBin("cgroup-join")),
		restartDelay:  envDur(*fRestartWait, "RESTART_DELAY", 5*time.Second),

		llmService:     envString(*fLLMSvc, "LLM_SERVICE", "llama"),
		llmRestartWait: envDur(*fLLMWait, "LLM_RESTART_WAIT", 3*time.Minute),
		restartWindow:  envDur(*fWin, "RESTART_WINDOW", 30*time.Minute),
		restartMax:     envInt(*fMax, "RESTART_MAX", 2),
		breakerFor:     envDur(*fBreaker, "BREAKER", 20*time.Second),
		failThreshold:  envInt(*fFails, "FAIL_THRESHOLD", 2),
		rcService:      envString(*fRCSvc, "RC_SERVICE", defaultRCService()),
	}
}

// validate refuses the configurations that cannot be true. The first is the sd
// command line (there is no sensible default for it); the rest are the invariants the
// slot model relies on: one slot per port, and one resident holder of a card per group.
func (c config) validate(slots []*slot) error {
	if len(c.sd.args) == 0 {
		return fmt.Errorf("no sd-server command line given: pass -sd-args \"...\" or set %sSD_ARGS", envPrefix)
	}
	ports := map[int]string{}
	for _, s := range slots {
		if s.spec.url == "" {
			continue
		}
		p := s.port()
		if p == 0 {
			return fmt.Errorf("%s: cannot read a port from %q", s.spec.name, s.spec.url)
		}
		if other, taken := ports[p]; taken {
			return fmt.Errorf("two slots claim port %d: %s and %s. A port belongs to exactly "+
				"one slot; move one of them (whisper is CPU-only and does not need 55557)",
				p, other, s.spec.name)
		}
		ports[p] = s.spec.name
	}
	card := map[string]string{}
	for _, s := range slots {
		if s.spec.policy != policyResident || !s.spec.vram || s.spec.group == "" {
			continue
		}
		if other, taken := card[s.spec.group]; taken {
			return fmt.Errorf("%s and %s would both keep the card in group %q: at most one "+
				"resident holder per exclusivity group", other, s.spec.name, s.spec.group)
		}
		card[s.spec.group] = s.spec.name
	}
	return nil
}

// buildSlots turns the configuration into the estate. This is the whole slot model in
// data: the lifecycle in slot.go is generic, and a fifth engine is a literal here.
func buildSlots(c config) []slotSpec {
	resident := func(name string, e engineConfig, group string, routed bool, idle time.Duration) slotSpec {
		own := e.bin != ""
		pol := policyResident
		if !own {
			pol = policyExternal
		}
		return slotSpec{
			name: name, url: e.url, bin: e.bin, args: e.args, env: e.env,
			group: group, vram: true, policy: pol, idle: idle,
			routed: routed, fallback: true, cgroup: e.cgroup,
		}
	}
	whisperPolicy := policyOnDemand
	if c.whisper.bin == "" {
		whisperPolicy = policyExternal // exists as a route, answers 503, never spawns
	}
	return []slotSpec{
		// The router: resident, and it keeps the card by keeping models.
		resident("llama", c.llama, "gpu", true, 0),
		// The media child: on demand, because it has no other way to give the card back.
		{
			name: "sd", url: c.sd.url, bin: c.sd.bin, args: c.sd.args, env: c.sd.env,
			group: "gpu", vram: true, policy: policyOnDemand, idle: c.idle,
			routed: true, fallback: false, cgroup: c.sd.cgroup,
		},
		// CPU-only and therefore in no group: it contends for neither the card nor a
		// conversation about it. Its port is its own business (55559), which is what
		// ends the 55557 collision instead of arbitrating it at runtime.
		{
			name: "whisper", url: c.whisper.url, bin: c.whisper.bin,
			args: c.whisper.args, env: c.whisper.env,
			group: "", vram: false, policy: whisperPolicy, idle: c.whisperIdle,
			routed: true, fallback: false, cgroup: c.whisper.cgroup,
		},
		// The second card's router: supervised, not routed. Its callers address 55556
		// directly; demesne owns it because something has to own the restart.
		resident("llamb", c.llamb, "aux", false, 0),
	}
}

// ----------------------------------------------------------------------- estate

type demesne struct {
	cfg   config
	slots []*slot
	index map[string]*slot

	transition sync.Mutex // one transition at a time; never held across a proxy
	proxies    map[string]*httputil.ReverseProxy
	llm        *llmWatch

	done    chan struct{}
	closing atomic.Bool
}

func newDemesne(cfg config) *demesne {
	d := &demesne{
		cfg:     cfg,
		index:   map[string]*slot{},
		proxies: map[string]*httputil.ReverseProxy{},
		llm:     newLLMWatch(),
		done:    make(chan struct{}),
	}
	for _, spec := range buildSlots(cfg) {
		s := &slot{spec: spec}
		d.slots = append(d.slots, s)
		d.index[spec.name] = s
	}
	for _, s := range d.slots {
		d.proxies[s.spec.name] = reverseProxy(d, s.spec.url, s.spec.name == "llama")
	}
	return d
}

func (d *demesne) slotByName(name string) *slot { return d.index[name] }

// ------------------------------------------------------------------- the card

// vramUsed reads the 20 GiB card's counter. The box has more than one GPU, and
// /dev/dri node numbers move between boots, so pick the device with the largest total
// rather than hardcoding a card index.
func vramUsed() (uint64, error) {
	totals, err := filepath.Glob("/sys/class/drm/card*/device/mem_info_vram_total")
	if err != nil || len(totals) == 0 {
		return 0, errors.New("no /sys/class/drm/card*/device/mem_info_vram_total")
	}
	var usedPath string
	var biggest uint64
	for _, p := range totals {
		t, err := readUint(p)
		if err != nil {
			continue
		}
		if t > biggest {
			biggest, usedPath = t, p
		}
	}
	if usedPath == "" {
		return 0, errors.New("no readable VRAM total")
	}
	return readUint(strings.TrimSuffix(usedPath, "_total") + "_used")
}

func readUint(path string) (uint64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
}

func vramString() string {
	used, err := vramUsed()
	if err != nil {
		return "unknown"
	}
	return fmt.Sprintf("%.3f GiB", float64(used)/(1<<30))
}

func waitVRAMBelow(limit uint64, howLong time.Duration) error {
	deadline := time.Now().Add(howLong)
	var last uint64
	for time.Now().Before(deadline) {
		used, err := vramUsed()
		if err == nil {
			last = used
			if used < limit {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("vram still %.3f GiB after %s (wanted below %.3f GiB)",
		float64(last)/(1<<30), howLong, float64(limit)/(1<<30))
}

// -------------------------------------------------------------------- routing

// routeKind is what a request may do to the estate.
type routeKind int

const (
	kindChat        routeKind = iota // the LLM path: wait for a render, evict the card, proxy
	kindMetadata                     // proxy untouched: the router loads nothing for these
	kindGeneration                   // may spawn the slot and re-arm its idle clock
	kindObservation                  // only ever reports on a slot: never spawns
)

type intent struct {
	slot string
	kind routeKind
}

// resolve is the routing table, in one place and as data about paths.
//
// The generation/observation split is not cosmetic: a monitoring loop polling
// /sdapi/v1/progress would otherwise spawn the child and pin the card forever, which
// is the exact thing the idle timer exists to undo. Metadata is proxied untouched
// because a picker polls /v1/models constantly and the router answers it without
// loading anything -- a transition there would kill a render on every UI refresh.
//
// Two fixes over stadi, both from the estate's own history:
//   - whisper is ROUTED now (/inference, /v1/audio/*). It used to match no route and
//     fall through to the default branch, i.e. to the llama router, which answered
//     with a 404 from the wrong engine.
//   - an unconfigured whisper answers 503 here instead of falling through to llama.
func resolve(p string) intent {
	switch {
	case p == "/v1/models", p == "/props", p == "/health", p == "/healthz":
		return intent{"llama", kindMetadata}
	case p == "/sdapi/v1/txt2img", p == "/sdapi/v1/img2img", strings.HasPrefix(p, "/v1/images/"):
		return intent{"sd", kindGeneration}
	case strings.HasPrefix(p, "/sdapi/"):
		return intent{"sd", kindObservation}
	case p == "/inference", strings.HasPrefix(p, "/v1/audio/"):
		return intent{"whisper", kindGeneration}
	}
	// Everything else is chat: /v1/chat/completions, /models/load, /models/unload, ...
	return intent{"llama", kindChat}
}

// reverseProxy builds the pass-through. isLLM adds the watch: a failed dial or a
// stream that broke mid-body is the strongest evidence about the router, and it
// arrives before any probe could fire, so the policy hangs off this edge.
func reverseProxy(d *demesne, base string, isLLM bool) *httputil.ReverseProxy {
	target, err := url.Parse(base)
	if err != nil {
		log.Fatalf("bad target %q: %v", base, err)
	}
	p := httputil.NewSingleHostReverseProxy(target)
	// -1 flushes every write straight through: chat is SSE, and sd streams
	// image_generation.* events.
	p.FlushInterval = -1
	p.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("%s %s -> %s: %v", r.Method, r.URL.Path, base, err)
		if isLLM {
			if restart := d.llm.fail(err, d.cfg.failThreshold, d.cfg.breakerFor); restart {
				d.recoverLLM(err.Error())
			}
		}
		http.Error(w, "demesne: upstream failed: "+err.Error(), http.StatusBadGateway)
	}
	if isLLM {
		// Any answer below 500 means the engine really answered -- a streamed 200 is
		// worth more than a health check, because it proves the model ran.
		p.ModifyResponse = func(resp *http.Response) error {
			if resp.StatusCode < 500 {
				d.llm.ok()
			}
			return nil
		}
	}
	return p
}

func (d *demesne) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path
	switch {
	case p == "/status":
		d.status(w)
	case p == "/estate/card":
		d.card(w, r)
	case p == "/release":
		d.handleRelease(w, r)
	case strings.HasPrefix(p, "/slots/"):
		d.handleSlotControl(w, r)
	default:
		d.route(w, r, resolve(p))
	}
}

func (d *demesne) route(w http.ResponseWriter, r *http.Request, in intent) {
	s := d.slotByName(in.slot)
	if s == nil {
		http.Error(w, "demesne: no such slot: "+in.slot, http.StatusInternalServerError)
		return
	}
	if !s.spec.own() && !s.spec.fallback {
		http.Error(w, fmt.Sprintf("demesne: %s is not configured (no command line: set -%s-args)",
			s.spec.name, s.spec.name), http.StatusServiceUnavailable)
		return
	}

	switch in.kind {
	case kindGeneration:
		if err := d.ensure(s); err != nil {
			if !s.spec.own() && !s.spec.fallback {
				http.Error(w, "demesne: "+err.Error(), http.StatusServiceUnavailable)
				return
			}
			log.Printf("%s request refused: %v", s.spec.name, err)
			http.Error(w, "demesne: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		atomic.AddInt64(&s.inflight, 1)
		d.proxies[s.spec.name].ServeHTTP(w, r)
		atomic.AddInt64(&s.inflight, -1)
		// Re-arm from the END of the request. ensure() touches BEFORE the render, and a
		// cold render takes ~8-10 min, so a 5-minute timer armed there would kill the
		// child mid-load. The count above is what tells the timer a render is running.
		s.armIdle(d)

	case kindObservation:
		// Never spawn for a read: answer honestly instead, so a monitor cannot hold the card.
		if up, _ := s.isUp(); !up || !portAlive(s.spec.url) {
			http.Error(w, "demesne: "+s.spec.name+" is not loaded (no child running; see /status)",
				http.StatusServiceUnavailable)
			return
		}
		d.proxies[s.spec.name].ServeHTTP(w, r)

	case kindMetadata:
		d.proxies[s.spec.name].ServeHTTP(w, r)

	default: // kindChat
		// The breaker: once the router has failed a burst of requests, refuse new ones
		// quickly instead of dialling a dead port once per client and letting each wait
		// out its own timeout. A successful request clears it.
		if open, wait := d.llm.failing(); open {
			w.Header().Set("Retry-After", strconv.Itoa(int(wait.Seconds())))
			http.Error(w, "demesne: the llama engine is failing; retry shortly (see /status)",
				http.StatusServiceUnavailable)
			return
		}
		// A resident router that is still loading is not broken: wait a bounded while
		// for its port rather than take a 502 from the proxy. Then say so plainly.
		if s.booting() {
			deadline := time.Now().Add(d.cfg.busyWait)
			for s.booting() && time.Now().Before(deadline) {
				time.Sleep(500 * time.Millisecond)
			}
			if s.booting() {
				http.Error(w, "demesne: the llama router is still starting; retry shortly",
					http.StatusServiceUnavailable)
				return
			}
		}
		// A chat request wants the card. If a render is in flight, wait for it to
		// finish -- then say so plainly instead of killing it.
		if sd := d.slotByName("sd"); sd != nil {
			if n := atomic.LoadInt64(&sd.inflight); n > 0 {
				deadline := time.Now().Add(d.cfg.busyWait)
				for atomic.LoadInt64(&sd.inflight) > 0 && time.Now().Before(deadline) {
					time.Sleep(500 * time.Millisecond)
				}
				if atomic.LoadInt64(&sd.inflight) > 0 {
					http.Error(w, "demesne: an image generation is in progress on the card; retry shortly",
						http.StatusServiceUnavailable)
					return
				}
			}
			if up, _ := sd.isUp(); up {
				d.evict(sd, "a chat request wants the card")
			}
		}
		d.proxies[s.spec.name].ServeHTTP(w, r)
	}
}

// ------------------------------------------------------------------- /status

type slotStatus struct {
	Name       string  `json:"name"`
	Listen     string  `json:"listen"`
	ListenPort int     `json:"port"`
	Group      string  `json:"group,omitempty"`
	Policy     string  `json:"policy"`
	Owned      bool    `json:"owned"`
	Up         bool    `json:"up"`
	Booting    bool    `json:"booting"`
	PID        int     `json:"pid,omitempty"`
	Foreign    string  `json:"port_held_elsewhere,omitempty"`
	InFlight   int64   `json:"in_flight"`
	IdleSecond float64 `json:"idle_timeout_s,omitempty"`
	UptimeSecs float64 `json:"uptime_s,omitempty"`
	Restarts   int     `json:"restarts"`
	LastError  string  `json:"last_error,omitempty"`
	Cgroup     string  `json:"cgroup,omitempty"`
}

func (d *demesne) slotStatuses() []slotStatus {
	out := make([]slotStatus, 0, len(d.slots))
	for _, s := range d.slots {
		out = append(out, slotStatusOf(s))
	}
	return out
}

// slotStatusOf reads one slot without the transition lock, so a status poll is never
// stuck behind an eight-minute cold load.
func slotStatusOf(s *slot) slotStatus {
	s.mu.Lock()
	st := slotStatus{
		Name: s.spec.name, Listen: s.spec.url, ListenPort: s.port(),
		Group: s.spec.group, Policy: s.spec.policy.String(), Owned: s.spec.own(),
		Up: s.up, Booting: s.up && s.readyAt.IsZero(), PID: s.pid,
		InFlight: atomic.LoadInt64(&s.inflight), Restarts: s.restarts,
		LastError: s.lastErr,
	}
	if s.spec.idle > 0 {
		st.IdleSecond = s.spec.idle.Seconds()
	}
	if s.up && !s.readyAt.IsZero() {
		st.UptimeSecs = time.Since(s.readyAt).Seconds()
	}
	if s.spec.cgroup.set() {
		st.Cgroup = s.spec.cgroup.leaf
	}
	foreign := s.foreign
	s.mu.Unlock()

	// An external engine is someone else's child, so "up" has to be asked of its port
	// rather than of our process table -- and it is never a foreign holder, because the
	// port is its own by contract. This matters during the migration: most engines are
	// external while demesne takes them over one at a time.
	if s.spec.policy == policyExternal {
		st.Up = portAliveWithin(s.spec.url, 150*time.Millisecond)
		st.Booting = false
		return st
	}

	// A port answered while the slot is down means someone else owns it. Bounded
	// short: /status is polled, and it must never be the slowest thing on the box.
	if !st.Up && portAliveWithin(s.spec.url, 150*time.Millisecond) {
		foreign = true
	}
	if foreign {
		st.Foreign = "a process demesne does not own"
	}
	return st
}

func (d *demesne) status(w http.ResponseWriter) {
	sd := d.slotByName("sd")
	up, pid := false, 0
	inflight := int64(0)
	if sd != nil {
		up, pid = sd.isUp()
		inflight = atomic.LoadInt64(&sd.inflight)
	}
	used, _ := vramUsed()
	out := map[string]any{
		// The estate, as slots.
		"version":         version,
		"listen":          d.cfg.listen,
		"socket":          d.cfg.socket,
		"vram_used_gib":   float64(used) / (1 << 30),
		"media_in_flight": inflight,
		"llm_health":      d.llm.snapshot(),
		"slots":           d.slotStatuses(),

		// stadi's field names, kept because DSH and the habit of a year read them.
		"slot":           slotName(up),
		"sd_child_up":    up,
		"sd_child_pid":   pid,
		"sd_listen":      d.cfg.sd.url,
		"llm":            d.cfg.llama.url,
		"idle_timeout_s": d.cfg.idle.Seconds(),
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
}

// slotName is stadi's one-bit answer to "who holds the card", kept as-is: it is what
// the old /status published and nothing gained by renaming it.
func slotName(sdUp bool) string {
	if sdUp {
		return "sd"
	}
	return "llm"
}

// ----------------------------------------------------------------- /estate/card

// estateCard is the typed answer to "who holds this card, and since when". It exists
// beside /status rather than inside it: a client that reacts to the card should not have
// to parse four kilobytes of operator detail to find two booleans, and every field it
// parses is a field it comes to depend on. The four fields below are the whole contract.
type estateCard struct {
	Group   string     `json:"group"`
	Holders []string   `json:"holders"`
	SinceMS int64      `json:"since_ms"`
	Slots   []cardSlot `json:"slots"`
}

type cardSlot struct {
	Name  string `json:"name"`
	State string `json:"state"`
}

// card answers for one exclusivity group, "gpu" by default.
//
// holders is an ARRAY, not one value, because the invariant is "at most one holder at
// rest": during a handover both engines can be up for the length of an eviction, and a
// client that cannot express that would have to guess which one is lying.
//
// since_ms is the earliest readiness among the current holders -- when the card became
// busy, not when this request was made -- and 0 when the group is idle or the holder is
// an engine demesne did not start (an external one has no moment of readiness that we
// observed, and inventing one would be a guess).
func (d *demesne) card(w http.ResponseWriter, r *http.Request) {
	group := r.URL.Query().Get("group")
	if group == "" {
		group = "gpu"
	}
	out := estateCard{Group: group, Holders: []string{}, Slots: []cardSlot{}}
	for _, s := range d.slots {
		if s.spec.group != group {
			continue
		}
		st := slotStatusOf(s)
		state := "stopped"
		switch {
		case st.Up && st.Booting:
			state = "loading"
		case st.Up:
			state = "running"
		}
		out.Slots = append(out.Slots, cardSlot{Name: s.spec.name, State: state})
		if state != "running" {
			continue
		}
		out.Holders = append(out.Holders, s.spec.name)
		if since := s.readySince().UnixMilli(); since > 0 && (out.SinceMS == 0 || since < out.SinceMS) {
			out.SinceMS = since
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// ------------------------------------------------------------------ /release

func (d *demesne) handleRelease(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("slot")
	if name != "" && d.slotByName(name) == nil {
		http.Error(w, "demesne: no such slot: "+name, http.StatusNotFound)
		return
	}
	released := []string{}
	for _, s := range d.slots {
		if name != "" && s.spec.name != name {
			continue
		}
		if s.spec.policy != policyOnDemand {
			continue
		}
		if up, _ := s.isUp(); !up {
			continue
		}
		d.evict(s, "operator release")
		released = append(released, s.spec.name)
	}
	if len(released) == 0 {
		fmt.Fprintln(w, "released nothing")
		return
	}
	fmt.Fprintf(w, "released %s\n", strings.Join(released, " "))
}

// handleSlotControl is the per-engine operator interface: /slots/<name>{,/status,
// /load,/stop}. It replaces "rc-service --user llama restart" as the way to touch one
// engine, because rc-service can no longer see a child demesne forked.
func (d *demesne) handleSlotControl(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/slots/")
	name, action := rest, ""
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		name, action = rest[:i], rest[i+1:]
	}
	s := d.slotByName(name)
	if s == nil {
		http.Error(w, "demesne: no such slot: "+name, http.StatusNotFound)
		return
	}
	switch action {
	case "", "status":
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(slotStatusOf(s))
	case "load", "start":
		if r.Method != http.MethodPost {
			http.Error(w, "demesne: POST required", http.StatusMethodNotAllowed)
			return
		}
		if err := d.ensure(s); err != nil {
			http.Error(w, "demesne: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, "%s loaded\n", s.spec.name)
	case "stop", "release":
		if r.Method != http.MethodPost {
			http.Error(w, "demesne: POST required", http.StatusMethodNotAllowed)
			return
		}
		d.evict(s, "operator stop")
		fmt.Fprintf(w, "%s stopped\n", s.spec.name)
	default:
		http.Error(w, "demesne: try /slots/<name>, /load or /stop", http.StatusNotFound)
	}
}

// --------------------------------------------------------------------- main

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("demesne: ")

	// A first non-flag argument is a subcommand: demesne status|release|load|stop.
	sub := ""
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		sub = os.Args[1]
		os.Args = append(os.Args[:1], os.Args[2:]...)
	}
	cfg := loadConfig()
	if sub != "" {
		runSubcommand(sub, cfg)
		return
	}

	d := newDemesne(cfg)
	if err := cfg.validate(d.slots); err != nil {
		log.Fatal(err)
	}

	log.Printf("listening on %s | llama %s | sd %s | whisper %s | llamb %s | idle %s | vram %s",
		cfg.listen, cfg.llama.url, cfg.sd.url, cfg.whisper.url, cfg.llamb.url, cfg.idle, vramString())
	if cfg.socket != "" {
		log.Printf("the same API is also on unix:%s (mode 0600)", cfg.socket)
	}
	for _, s := range d.slots {
		note := ""
		if s.spec.policy == policyExternal {
			note = " (external: routed, not owned)"
		}
		log.Printf("slot %-8s %-24s policy=%-10s group=%-4s%v",
			s.spec.name, s.spec.url, s.spec.policy, orNone(s.spec.group), note)
	}

	// A port answering before demesne started the engine is either a leftover or
	// another supervisor; either way it is the collision this program refuses to race.
	for _, s := range d.slots {
		if s.spec.url == "" || s.spec.policy == policyExternal {
			continue
		}
		if portAlive(s.spec.url) {
			log.Printf("warning: %s is already answering on %s; demesne did not start it",
				s.spec.name, s.spec.url)
		}
	}

	for _, s := range d.slots {
		d.prepareCgroup(s)
	}

	// supervise-daemon stops us with SIGTERM. Because every child sits in its own
	// process group it would NOT die with us, and an orphaned sd-server is exactly the
	// 12.5 GiB hostage this program exists to prevent -- so take them all down first.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-stop
		d.closing.Store(true)
		close(d.done)
		log.Printf("signal received; stopping the card's children and exiting")
		d.stopAll("demesne is going away")
		if d.cfg.socket != "" {
			os.Remove(d.cfg.socket)
		}
		os.Exit(0)
	}()

	for _, s := range d.slots {
		if s.spec.policy == policyResident {
			go d.supervise(s)
		}
	}

	// One handler, two transports. The port is for DSH, which we do not control and which
	// reaches :55558 by name; the socket is for anything running as this user, where a path
	// and a file mode say who may connect and no port can be contended for.
	srv := &http.Server{
		Handler:           d,
		ReadHeaderTimeout: 15 * time.Second,
		// No WriteTimeout: an image generation can legitimately take ten minutes, and a
		// default would guillotine it.
	}

	tcp, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		log.Fatal(err)
	}
	listeners := []net.Listener{tcp}
	if cfg.socket != "" {
		ln, err := listenUnix(cfg.socket)
		if err != nil {
			// Never fatal: a box that cannot have a socket still has to serve the card.
			log.Printf("warning: no unix socket: %v; the port is the only way in", err)
		} else {
			log.Printf("also serving on unix:%s (mode 0600)", cfg.socket)
			listeners = append(listeners, ln)
		}
	}
	for _, ln := range listeners {
		go func(ln net.Listener) {
			if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("listener %s stopped: %v", ln.Addr(), err)
			}
		}(ln)
	}
	select {}
}

// listenUnix prepares the local channel: the same handler demesne serves on its port,
// on a socket any process running as this user can reach.
//
// Why a socket and not a second protocol: everything that would have to change to take
// HTTP off this path is shared with DSH, which is not ours to change -- the same handler
// answers both, so the OpenAI dialect, SSE streaming and the slot control endpoints are
// one implementation either way. What a socket buys that a protocol cannot is the absence
// of a port (nobody can collide with it, so the 55557 class of bug is gone by
// construction) and an access control that is the filesystem: mode 0600, no token, no
// loopback listener that any local user can dial.
//
// A stale socket file is removed only when nothing answers on it, because bind(2) refuses
// a path that exists and unlinking a live daemon's socket is how you get two of them.
func listenUnix(path string) (net.Listener, error) {
	if path == "" {
		return nil, errors.New("no socket path")
	}
	if fi, err := os.Stat(path); err == nil {
		if fi.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("%s exists and is not a socket", path)
		}
		if c, err := net.DialTimeout("unix", path, 500*time.Millisecond); err == nil {
			c.Close()
			return nil, fmt.Errorf("%s is already served by another process", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("remove stale socket: %w", err)
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		os.Remove(path)
		return nil, err
	}
	return ln, nil
}

func orNone(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
