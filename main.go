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
//	                story tenant.go settles.
//
// The rules are in tenant.go; this file is configuration, routing, /status and the
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
	"context"
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
	"sort"
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
	models []string
	// dir is the child's working directory. It is not cosmetic: a router's models.ini
	// names its weights relative to the models directory (`model = ./d1-2B-Q8_0.gguf`,
	// `mmproj = ../asset/...`), so a router started anywhere else loads nothing.
	dir    string
	cgroup memLimit
}

type config struct {
	listen                            string // where demesne itself listens (the port DSH uses)
	socket                            string // the same API on a unix socket; empty = no socket
	confFile                          string // what SIGHUP re-reads; empty = nothing to reload from
	tenantsFile                       string // tenants.ini describing every engine; empty = the built-in five
	sd, llama, llamb, whisper, strata engineConfig
	idle                              time.Duration // evict an on-demand child after this
	whisperIdle                       time.Duration
	busyWait                          time.Duration // a chat request waits this long for a render
	vramWait                          time.Duration // bound on waiting for the card to return
	loadWait                          time.Duration // bound on a child's port appearing
	freeBelow                         uint64        // bytes: "the card is free" threshold
	orphanAbove                       uint64        // bytes: VRAM still held while the port is dead
	logDir                            string
	cgroupRoot                        string
	cgroupPrepare                     string // cgroup-prepare helper (root, via sudo -n)
	cgroupJoin                        string
	restartDelay                      time.Duration // first delay between resident restarts

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
// The estate's ports, in one place. demesne sits on the number every caller already means
// and the engines keep their own; a reload and a boot read these same defaults.
const (
	cgroupMount       = "/sys/fs/cgroup"
	cgroupRootDefault = "/sys/fs/cgroup/estate"
	defaultLlamaURL   = "http://127.0.0.1:55556"
	defaultLlambURL   = "http://127.0.0.1:55557"
	defaultSDURL      = "http://127.0.0.1:55558"
	defaultWhisperURL = "http://127.0.0.1:55559"
)

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

// modelList is a tenant's configured model ids: a value that names an existing file is read as
// one id per line (blank lines and #comments ignored), anything else is split as words. So a
// fixed set can live in the conf as one line or in a file the conf points at.
func modelList(v string) []string {
	if v == "" {
		return nil
	}
	if st, err := os.Stat(v); err == nil && st.Mode().IsRegular() {
		b, err := os.ReadFile(v)
		if err != nil {
			return nil
		}
		var out []string
		for _, line := range strings.Split(string(b), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			out = append(out, strings.Fields(line)...)
		}
		return out
	}
	return fields(v)
}

// memLimitFor reads a tenant's memory fence from the environment. Env-only on purpose:
// these are set once in conf.d/demesne, never typed on a command line, and there are
// three of them per tenant.
func memLimitFor(root, tenant string, get func(string) string) memLimit {
	// The key is the tenant's name UPPERCASED. It was not, which is why every fence in the
	// estate was silently unlimited: DEMESNE_sd_MEMORY_HIGH is not a variable anybody sets.
	key := strings.ToUpper(tenant)
	m := memLimit{
		leaf:     filepath.Join(root, cgroupLeaf(tenant)),
		high:     get(key + "_MEMORY_HIGH"),
		max:      get(key + "_MEMORY_MAX"),
		swapHigh: get(key + "_SWAP_HIGH"),
	}
	if m.high == "" && m.max == "" && m.swapHigh == "" {
		return memLimit{}
	}
	return m
}

// llamaURL keeps the tenant's name on the tenant's URL. Every other knob of that engine is
// DEMESNE_LLAMA_*, so DEMESNE_LLM was the one name that did not match the thing it named --
// and a value silently accepted under an old spelling is how an afternoon gets spent
// wondering why the documented one did nothing.
func llamaURL(flagVal string) string {
	if v := envString(flagVal, "LLAMA", ""); v != "" {
		return v
	}
	if v := envRaw("LLM"); v != "" {
		log.Printf("note: DEMESNE_LLM is the old spelling of DEMESNE_LLAMA; using it, but the tenant's name is the one to set")
		return v
	}
	return defaultLlamaURL
}

func envRaw(key string) string {
	if v := os.Getenv(envPrefix + key); v != "" {
		return v
	}
	return os.Getenv(legacyPrefix + strings.ToUpper(key))
}

// envFromMap is envRaw over a parsed configuration file instead of the process
// environment. A running process cannot see an edited conf.d through os.Getenv -- the
// environment is fixed at exec -- so a reload reads the file with a shell and then asks
// the same questions of the answer.
func envFromMap(m map[string]string) func(string) string {
	return func(key string) string {
		if v := m[envPrefix+key]; v != "" {
			return v
		}
		return m[legacyPrefix+strings.ToUpper(key)]
	}
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
	// A service started by supervise-daemon has a PATH without the user's own bin
	// directory, and that is where this box keeps cgroup-prepare and cgroup-join. Looking
	// there is the difference between a fence and a warning on every boot: the first
	// version of this only asked PATH, found nothing, and quietly ran the whole estate
	// unlimited with a log line nobody reads.
	if home, err := os.UserHomeDir(); err == nil {
		for _, dir := range []string{filepath.Join(home, "bin"), "/usr/local/bin", "/usr/bin"} {
			p := filepath.Join(dir, name)
			if fi, err := os.Stat(p); err == nil && !fi.IsDir() && fi.Mode()&0o111 != 0 {
				return p
			}
		}
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
		fConf    = flag.String("conf", "", "the configuration file SIGHUP re-reads (env "+envPrefix+"CONF)")
		fTenants = flag.String("tenants", "", "file describing every tenant (env "+envPrefix+"TENANTS)")

		fLLM     = flag.String("llm", "", "the llama router's base URL (env "+envPrefix+"LLM)")
		fSD      = flag.String("sd", "", "URL the sd child comes up on (env "+envPrefix+"SD)")
		fLlamb   = flag.String("llamb", "", "the auxiliary router's base URL (env "+envPrefix+"LLAMB)")
		fWhisper = flag.String("whisper", "", "URL the whisper child comes up on (env "+envPrefix+"WHISPER)")
		fStrata  = flag.String("strata", "", "the Strata engine's base URL (env "+envPrefix+"STRATA)")

		fIdle        = flag.String("idle", "", "evict an on-demand child after this long with no request (env "+envPrefix+"IDLE)")
		fWhisperIdle = flag.String("whisper-idle", "", "the same for whisper (env "+envPrefix+"WHISPER_IDLE)")
		fBusyWait    = flag.String("busy-wait", "", "how long a chat request waits for a render (env "+envPrefix+"BUSY_WAIT)")
		fVramWait    = flag.String("vram-wait", "", "bound on waiting for the card to come back (env "+envPrefix+"VRAM_WAIT)")
		fLoadWait    = flag.String("load-wait", "", "bound on a child's port appearing (env "+envPrefix+"LOAD_WAIT)")
		fFreeGiB     = flag.Float64("free-gib", 2, "treat the card as free below this many GiB used")
		fRestartWait = flag.String("restart-delay", "", "first delay between resident restarts (env "+envPrefix+"RESTART_DELAY)")

		fLogDir    = flag.String("log-dir", "", "directory for demesne's and the children's logs (env "+envPrefix+"LOG_DIR)")
		fSDLogDir  = flag.String("sd-log-dir", "", "deprecated: use -log-dir")
		fCgroup    = flag.String("cgroup-root", "", "delegated cgroup v2 subtree for the tenants (env "+envPrefix+"CGROUP_ROOT)")
		fCgPrepare = flag.String("cgroup-prepare", "", "cgroup-prepare helper, run via sudo -n (env "+envPrefix+"CGROUP_PREPARE)")
		fCgJoin    = flag.String("cgroup-join", "", "cgroup-join helper, run via sudo -n (env "+envPrefix+"CGROUP_JOIN)")

		fSDBin  = flag.String("sd-bin", "", "sd-server binary (env "+envPrefix+"SD_BIN; default: PATH)")
		fSDArgs = flag.String("sd-args", "", "the sd child's whole command line (env "+envPrefix+"SD_ARGS)")
		fSDEnv  = flag.String("sd-env", "", "extra KEY=VALUE for the sd child (env "+envPrefix+"SD_ENV)")
		fSDDir  = flag.String("sd-dir", "", "the sd child's working directory (env "+envPrefix+"SD_DIR)")

		fLlamaBin  = flag.String("llama-bin", "", "adopt the llama router: its binary (env "+envPrefix+"LLAMA_BIN; empty = leave it to OpenRC)")
		fLlamaArgs = flag.String("llama-args", "", "the router's whole command line (env "+envPrefix+"LLAMA_ARGS)")
		fLlamaEnv  = flag.String("llama-env", "", "extra KEY=VALUE for the router (env "+envPrefix+"LLAMA_ENV)")
		fLlamaDir  = flag.String("llama-dir", "", "the router's working directory: where its models.ini lives (env "+envPrefix+"LLAMA_DIR)")

		fLlambBin  = flag.String("llamb-bin", "", "adopt the aux router: its binary (env "+envPrefix+"LLAMB_BIN; empty = leave it to OpenRC)")
		fLlambArgs = flag.String("llamb-args", "", "the aux router's whole command line (env "+envPrefix+"LLAMB_ARGS)")
		fLlambEnv  = flag.String("llamb-env", "", "extra KEY=VALUE for the aux router (env "+envPrefix+"LLAMB_ENV)")
		fLlambDir  = flag.String("llamb-dir", "", "the aux router's working directory (env "+envPrefix+"LLAMB_DIR)")

		fWhisperBin  = flag.String("whisper-bin", "", "adopt whisper: its binary (env "+envPrefix+"WHISPER_BIN)")
		fWhisperArgs = flag.String("whisper-args", "", "whisper's whole command line (env "+envPrefix+"WHISPER_ARGS)")
		fWhisperEnv  = flag.String("whisper-env", "", "extra KEY=VALUE for whisper (env "+envPrefix+"WHISPER_ENV)")
		fWhisperDir  = flag.String("whisper-dir", "", "whisper's working directory (env "+envPrefix+"WHISPER_DIR)")

		fStrataBin  = flag.String("strata-bin", "", "adopt Strata: its binary (env "+envPrefix+"STRATA_BIN; empty = route only, never spawn)")
		fStrataArgs = flag.String("strata-args", "", "Strata's whole command line (env "+envPrefix+"STRATA_ARGS)")
		fStrataEnv  = flag.String("strata-env", "", "extra KEY=VALUE for Strata (env "+envPrefix+"STRATA_ENV)")
		fStrataDir  = flag.String("strata-dir", "", "Strata's working directory (env "+envPrefix+"STRATA_DIR)")

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
	cgroupRoot := envString(*fCgroup, "CGROUP_ROOT", cgroupRootDefault)

	return config{
		listen:      envString(*fListen, "LISTEN", "127.0.0.1:55558"),
		socket:      envString(*fSocket, "SOCKET", ""),
		confFile:    envString(*fConf, "CONF", ""),
		tenantsFile: envString(*fTenants, "TENANTS", ""),
		sd: engineConfig{
			url:    envString(*fSD, "SD", defaultSDURL),
			bin:    envString(*fSDBin, "SD_BIN", defaultBin("sd-server")),
			args:   fields(envString(*fSDArgs, "SD_ARGS", "")),
			env:    fields(envString(*fSDEnv, "SD_ENV", "")),
			dir:    envString(*fSDDir, "SD_DIR", ""),
			models: modelList(envRaw("SD_MODELS")),
			cgroup: memLimitFor(cgroupRoot, "sd", envRaw),
		},
		llama: engineConfig{
			url:    llamaURL(*fLLM),
			bin:    envString(*fLlamaBin, "LLAMA_BIN", ""),
			args:   fields(envString(*fLlamaArgs, "LLAMA_ARGS", "")),
			env:    fields(envString(*fLlamaEnv, "LLAMA_ENV", "")),
			dir:    envString(*fLlamaDir, "LLAMA_DIR", ""),
			models: modelList(envRaw("LLAMA_MODELS")),
			cgroup: memLimitFor(cgroupRoot, "llama", envRaw),
		},
		llamb: engineConfig{
			url:    envString(*fLlamb, "LLAMB", defaultLlambURL),
			bin:    envString(*fLlambBin, "LLAMB_BIN", ""),
			args:   fields(envString(*fLlambArgs, "LLAMB_ARGS", "")),
			env:    fields(envString(*fLlambEnv, "LLAMB_ENV", "")),
			dir:    envString(*fLlambDir, "LLAMB_DIR", ""),
			models: modelList(envRaw("LLAMB_MODELS")),
			cgroup: memLimitFor(cgroupRoot, "llamb", envRaw),
		},
		whisper: engineConfig{
			url:    envString(*fWhisper, "WHISPER", defaultWhisperURL),
			bin:    envString(*fWhisperBin, "WHISPER_BIN", ""),
			args:   fields(envString(*fWhisperArgs, "WHISPER_ARGS", "")),
			env:    fields(envString(*fWhisperEnv, "WHISPER_ENV", "")),
			dir:    envString(*fWhisperDir, "WHISPER_DIR", ""),
			models: modelList(envRaw("WHISPER_MODELS")),
			cgroup: memLimitFor(cgroupRoot, "whisper", envRaw),
		},
		strata: engineConfig{
			url:    envString(*fStrata, "STRATA", ""),
			bin:    envString(*fStrataBin, "STRATA_BIN", ""),
			args:   fields(envString(*fStrataArgs, "STRATA_ARGS", "")),
			env:    fields(envString(*fStrataEnv, "STRATA_ENV", "")),
			dir:    envString(*fStrataDir, "STRATA_DIR", ""),
			models: modelList(envRaw("STRATA_MODELS")),
			cgroup: memLimitFor(cgroupRoot, "strata", envRaw),
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
// tenant model relies on: one tenant per port, and one resident holder of a card per group.
func (c config) validate(tenants []*tenant) error {
	if len(c.sd.args) == 0 {
		return fmt.Errorf("no sd-server command line given: pass -sd-args \"...\" or set %sSD_ARGS", envPrefix)
	}
	ports := map[int]string{}
	for _, s := range tenants {
		if s.specOf().url == "" {
			continue
		}
		// A unix tenant has no port to read, claim or collide: its address is a path, and two
		// tenants cannot collide on one because the launcher refuses a path that answers.
		if unixPath(s.specOf().url) != "" {
			continue
		}
		p := s.port()
		if p == 0 {
			return fmt.Errorf("%s: cannot read a port from %q", s.specOf().name, s.specOf().url)
		}
		if other, taken := ports[p]; taken {
			return fmt.Errorf("two tenants claim port %d: %s and %s. A port belongs to exactly "+
				"one tenant; move one of them (whisper is CPU-only and does not need 55557)",
				p, other, s.specOf().name)
		}
		ports[p] = s.specOf().name
	}
	card := map[string]string{}
	for _, s := range tenants {
		if s.specOf().policy != policyResident || !s.specOf().vram || s.specOf().group == "" {
			continue
		}
		if other, taken := card[s.specOf().group]; taken {
			return fmt.Errorf("%s and %s would both keep the card in group %q: at most one "+
				"resident holder per exclusivity group", other, s.specOf().name, s.specOf().group)
		}
		card[s.specOf().group] = s.specOf().name
	}
	return nil
}

// buildTenants turns the configuration into the estate. This is the whole tenant model in
// data: the lifecycle in tenant.go is generic, and a fifth engine is a literal here.
func buildTenants(c config) []tenantSpec {
	resident := func(name string, e engineConfig, group string, routed bool, idle time.Duration) tenantSpec {
		own := e.bin != ""
		pol := policyResident
		if !own {
			pol = policyExternal
		}
		return tenantSpec{
			name: name, url: e.url, bin: e.bin, args: e.args, env: e.env, dir: e.dir,
			models: e.models, kind: kindModel,
			group: group, vram: true, policy: pol, idle: idle,
			routed: routed, router: true, fallback: true, cgroup: e.cgroup,
		}
	}
	whisperPolicy := policyOnDemand
	if c.whisper.bin == "" {
		whisperPolicy = policyExternal // exists as a route, answers 503, never spawns
	}
	strataPolicy := policyExternal
	if c.strata.bin != "" {
		strataPolicy = policyOnDemand
	}
	tenants := []tenantSpec{
		// The router: resident, and it keeps the card by keeping models.
		resident("llama", c.llama, "gpu", true, 0),
		// The media child: on demand, because it has no other way to give the card back.
		{
			name: "sd", url: c.sd.url, bin: c.sd.bin, args: c.sd.args, env: c.sd.env, dir: c.sd.dir,
			kind:  kindMedia,
			group: "gpu", vram: true, policy: policyOnDemand, idle: c.idle,
			routed: true, fallback: false, cgroup: c.sd.cgroup,
		},
		// CPU-only and therefore in no group: it contends for neither the card nor a
		// conversation about it. Its port is its own business (55559), which is what
		// ends the 55557 collision instead of arbitrating it at runtime.
		{
			name: "whisper", url: c.whisper.url, bin: c.whisper.bin,
			args: c.whisper.args, env: c.whisper.env, dir: c.whisper.dir,
			kind:  kindAudio,
			group: "", vram: false, policy: whisperPolicy, idle: c.whisperIdle,
			routed: true, fallback: false, cgroup: c.whisper.cgroup,
		},
		// The second card's router: routed, because its models are now reachable by name
		// through the one port. It is in its own group, so a request that lands here
		// never evicts a render from the big card.
		resident("llamb", c.llamb, "aux", true, 0),
	}
	if c.strata.url != "" {
		tenants = append(tenants, tenantSpec{
			name: "strata", url: c.strata.url, bin: c.strata.bin,
			args: c.strata.args, env: c.strata.env, dir: c.strata.dir,
			kind:  kindModel,
			group: "gpu", vram: true, policy: strataPolicy, idle: c.idle,
			routed: true, router: true, fallback: c.strata.bin == "", cgroup: c.strata.cgroup,
		})
	}
	return tenants
}

// ----------------------------------------------------------------------- estate

type demesne struct {
	cfg     config
	tenants []*tenant
	index   map[string]*tenant

	transition sync.Mutex // one transition at a time; never held across a proxy
	proxies    map[string]*httputil.ReverseProxy
	// probes is the same proxy without the health watch. A request that does not claim the
	// card must not be able to condemn the engine either: a monitoring loop polling /metrics
	// during a restart would otherwise trip the breaker and ask for a restart of an engine
	// that was simply starting -- the same mistake the /v1/models handler made once.
	probes  map[string]*httputil.ReverseProxy
	llm     *llmWatch // the default tenant's watch: /status keeps its shape
	watchMu sync.Mutex
	watches map[string]*llmWatch
	models  modelIndex // which engine advertises which model (models.go)
	loads   *loadBoard // which model each router is loading, if any (loads.go)
	hub     *eventHub  // the subscription feed (events.go)
	history *history   // the last transitions, for the cards

	startedAt   time.Time
	tenantNotes []string
	tenantsErr  error
	specNotes   []string
	specErr     error

	done    chan struct{}
	closing atomic.Bool
}

func newDemesne(cfg config) *demesne {
	d := &demesne{
		cfg:       cfg,
		index:     map[string]*tenant{},
		proxies:   map[string]*httputil.ReverseProxy{},
		probes:    map[string]*httputil.ReverseProxy{},
		watches:   map[string]*llmWatch{},
		loads:     newLoadBoard(cfg.logDir),
		hub:       newEventHub(),
		history:   &history{},
		startedAt: time.Now(),
		done:      make(chan struct{}),
	}
	d.models = newModelIndex()
	specs, notes, err := tenantsFromConfig(cfg)
	d.tenantNotes, d.tenantsErr = notes, err
	if err != nil {
		specs = sortedTenants(buildTenants(cfg))
	}
	for _, spec := range specs {
		s := &tenant{spec: spec, owner: d}
		d.tenants = append(d.tenants, s)
		d.index[spec.name] = s
	}
	// Declared models route from the first request, before any refresh: an on-demand engine
	// that has never answered must still be nameable.
	d.seedModels()
	if s := d.defaultTenant(); s != nil {
		d.llm = d.watchFor(s.specOf().name)
	} else {
		d.llm = newLLMWatch()
	}
	for _, s := range d.tenants {
		d.proxies[s.specOf().name] = reverseProxy(d, s, s.specOf().router)
		d.probes[s.specOf().name] = reverseProxy(d, s, false)
		// A resident tenant is somebody's responsibility from the start. A reload can hand
		// an engine back to OpenRC and take it again, so the flag is a real one and not a
		// property of the spec.
		if s.specOf().policy == policyResident || s.specOf().policy == policyKeep {
			s.setSupervised(true)
		}
	}
	return d
}

func (d *demesne) tenantByName(name string) *tenant { return d.index[name] }

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
	kindChat        routeKind = iota // a request that WANTS the card: wait for a render, evict it, proxy
	kindProxy                        // proxied to an engine that needs nothing: no card, no eviction
	kindMetadata                     // proxy untouched: the router loads nothing for these
	kindGeneration                   // may spawn the tenant and re-arm its idle clock
	kindObservation                  // only ever reports on a tenant: never spawns
)

type intent struct {
	tenant string
	kind   routeKind
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
	case p == "/v1/chat/completions", p == "/v1/completions", p == "/v1/embeddings",
		p == "/v1/rerank", p == "/v1/reranking", p == "/v1/systemone",
		p == "/completion", p == "/infill", p == "/embedding", p == "/reranking",
		p == "/models/load", p == "/models/unload",
		strings.HasPrefix(p, "/v1/chat/completions/"):
		return intent{"llama", kindChat}
	}
	// Anything else that reaches llama is proxied WITHOUT claiming the card: /tenants, /metrics,
	// /tokenize, /detokenize, /apply-template and whatever the next llama.cpp build adds. The
	// catch-all used to be chat, which meant a monitoring probe of /tenants on the front door
	// evicted a running render -- a probe is not a claim. What does claim is enumerated above,
	// because a path nobody recognised must fail safe: a request that quietly loses its card
	// claim is a slow load, while a probe that quietly takes the card kills someone's render.
	return intent{"llama", kindProxy}
}

// reverseProxy builds the pass-through. isLLM adds the watch: a failed dial or a
// stream that broke mid-body is the strongest evidence about the router, and it
// arrives before any probe could fire, so the policy hangs off this edge.
func reverseProxy(d *demesne, s *tenant, watch bool) *httputil.ReverseProxy {
	base := s.specOf().url
	watcher := d.watchFor(s.specOf().name)
	target, err := url.Parse(base)
	if err != nil {
		log.Fatalf("bad target %q: %v", base, err)
	}
	var p *httputil.ReverseProxy
	if path := unixPath(base); path != "" {
		// The stdlib transport picks its network from the URL scheme, so a `unix:` target is
		// rewritten to a placeholder http URL and every connection goes to the socket.
		placeholder, _ := url.Parse("http://unix-socket")
		p = httputil.NewSingleHostReverseProxy(placeholder)
		p.Transport = unixDialer(path)
	} else {
		p = httputil.NewSingleHostReverseProxy(target)
	}
	// -1 flushes every write straight through: chat is SSE, and sd streams
	// image_generation.* events.
	p.FlushInterval = -1
	p.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		cancelled := clientWentAway(r, err)
		if cancelled {
			// The caller gave up. That says nothing about the engine, and on 2026-11-07
			// counting it as evidence was enough to condemn a router that was 14.65 GiB
			// into loading a model: two client timeouts five minutes apart tripped the
			// breaker, the watch killed the child, and the load died with it.
			log.Printf("%s %s -> %s: the client went away (%v); not held against the engine",
				r.Method, r.URL.Path, base, err)
			return
		}
		log.Printf("%s %s -> %s: %v", r.Method, r.URL.Path, base, err)
		if watch {
			if restart := watcher.fail(err, d.cfg.failThreshold, d.cfg.breakerFor); restart {
				d.recoverLLM(s, err.Error())
			}
		}
		http.Error(w, "demesne: upstream failed: "+err.Error(), http.StatusBadGateway)
	}
	if watch {
		// Any answer below 500 means the engine really answered -- a streamed 200 is
		// worth more than a health check, because it proves the model ran.
		p.ModifyResponse = func(resp *http.Response) error {
			if resp.StatusCode < 500 {
				watcher.ok()
			}
			return nil
		}
	}
	return p
}

// clientWentAway separates "the engine failed" from "the caller stopped listening". The
// proxy sees both as a returned error, and only one of them is evidence about the engine.
func clientWentAway(r *http.Request, err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	select {
	case <-r.Context().Done():
		return true
	default:
		return false
	}
}

func (d *demesne) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path
	switch {
	case p == "/status":
		d.status(w)
	case p == "/estate/card":
		d.card(w, r)
	case p == "/v1/models":
		d.modelsHandler(w, r)
	case p == "/reload" && r.Method == http.MethodPost:
		d.handleReload(w, r)
	case p == "/v1/fim/completions":
		d.handleFIM(w, r)
	case p == "/rpc":
		d.handleRPC(w, r)
	case p == "/estate/metrics":
		d.handleMetrics(w, r)
	case p == "/estate/events":
		d.handleEvents(w, r)
	case p == "/release":
		d.handleRelease(w, r)
	case p == "/slots", strings.HasPrefix(p, "/slots/"):
		d.handleSlotsPath(w, r)
	case strings.HasPrefix(p, "/tenants/"):
		d.handleTenantControl(w, r)
	default:
		d.route(w, r, resolve(p))
	}
}

func (d *demesne) route(w http.ResponseWriter, r *http.Request, in intent) {
	// A chat request names the model it wants, and the model decides the engine: demesne
	// answers on the number every caller already means, so the name has to choose. The
	// path still decides the KIND of request; only the tenant moves.
	var pk peeked
	if in.kind == kindChat {
		pk = peekRequest(r)
		name := d.tenantForModel(pk.model)
		if name != "" {
			in.tenant = name
		}
		// A model that no tenant declares or advertises is a 404, not a silent fall through to
		// the default engine: on 2026-11-09 a request for a stopped on-demand engine's model
		// reached llama, which failed to load a name absent from its own models.ini and then
		// restored its previous model, putting an unrelated d1 back on the card. The request
		// was well formed; the model was absent.
		if pk.model != "" && name == "" {
			writeModelNotFound(w, pk.model, d.knownModels())
			return
		}
		if pk.model != "" && d.index[in.tenant].specOf().own() {
			d.loads.noteRequest(in.tenant, pk.model)
		}
	}
	s := d.tenantByName(in.tenant)
	if s == nil {
		http.Error(w, "demesne: no such tenant: "+in.tenant, http.StatusInternalServerError)
		return
	}
	if !s.specOf().own() && !s.specOf().fallback {
		http.Error(w, fmt.Sprintf("demesne: %s is not configured (no command line: set -%s-args)",
			s.specOf().name, s.specOf().name), http.StatusServiceUnavailable)
		return
	}
	if in.kind != kindProxy && in.kind != kindMetadata {
		s.noteUse()
		s.requests.Add(1)
		s.active.Add(1)
		t0 := time.Now()
		defer func() {
			s.active.Add(-1)
			s.lastLatency.Store(time.Since(t0).Microseconds())
		}()
	}

	switch in.kind {
	case kindGeneration:
		if err := d.ensure(s); err != nil {
			if !s.specOf().own() && !s.specOf().fallback {
				http.Error(w, "demesne: "+err.Error(), http.StatusServiceUnavailable)
				return
			}
			log.Printf("%s request refused: %v", s.specOf().name, err)
			http.Error(w, "demesne: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		atomic.AddInt64(&s.inflight, 1)
		d.proxies[s.specOf().name].ServeHTTP(w, r)
		atomic.AddInt64(&s.inflight, -1)
		// Re-arm from the END of the request. ensure() touches BEFORE the render, and a
		// cold render takes ~8-10 min, so a 5-minute timer armed there would kill the
		// child mid-load. The count above is what tells the timer a render is running.
		s.armIdle(d)

	case kindObservation:
		// Never spawn for a read: answer honestly instead, so a monitor cannot hold the card.
		if up, _ := s.isUp(); !up || !portAlive(s.specOf().url) {
			http.Error(w, "demesne: "+s.specOf().name+" is not loaded (no child running; see /status)",
				http.StatusServiceUnavailable)
			return
		}
		d.proxies[s.specOf().name].ServeHTTP(w, r)

	case kindMetadata, kindProxy:
		d.probes[s.specOf().name].ServeHTTP(w, r)

	default: // kindChat
		if !d.claimCard(w, r, s) {
			return
		}
		if pk.stream {
			// The stream opens first and keeps itself alive while the engine starts, so the
			// spawn runs beside it rather than in front of it: a cold 27B must not hold the
			// first byte for a minute.
			if s.specOf().own() {
				go d.spawnForStream(s)
			}
			d.streamChat(w, r, s, pk.body)
			return
		}
		if s.specOf().own() {
			err := d.ensure(s)
			if err != nil {
				log.Printf("%s request refused: %v", s.specOf().name, err)
				http.Error(w, "demesne: "+err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		d.proxies[s.specOf().name].ServeHTTP(w, r)
	}
}

// watchFor is one breaker per engine: a model that crash-loops must not refuse requests to its
// neighbours, which is the whole point of one process per model.
func (d *demesne) watchFor(name string) *llmWatch {
	d.watchMu.Lock()
	defer d.watchMu.Unlock()
	if w, ok := d.watches[name]; ok {
		return w
	}
	w := newLLMWatch()
	d.watches[name] = w
	return w
}

// claimCard is the preflight every request that WANTS the card needs, whichever dialect it
// arrived in: the router's breaker, the bounded wait for a booting engine, and the eviction of
// the media child when it shares the group. It writes its own refusal and reports whether the
// caller may proceed.
func (d *demesne) claimCard(w http.ResponseWriter, r *http.Request, s *tenant) bool {
	// The breaker is a property of an ENGINE, not of the tenant called llama: any tenant that
	// advertises models is one, so a per-model tenant gets a breaker too instead of silently
	// having none. It is still one shared watch; per-tenant watches are the follow-up.
	if s.specOf().router {
		if open, wait := d.watchFor(s.specOf().name).failing(); open {
			w.Header().Set("Retry-After", strconv.Itoa(int(wait.Seconds())))
			http.Error(w, "demesne: the "+s.specOf().name+
				" engine is failing; retry shortly (see /status)", http.StatusServiceUnavailable)
			return false
		}
	}
	// A resident router that is still loading is not broken: wait a bounded while for its
	// port rather than take a 502 from the proxy. Then say so plainly.
	if s.booting() {
		deadline := time.Now().Add(d.cfg.busyWait)
		for s.booting() && time.Now().Before(deadline) {
			time.Sleep(500 * time.Millisecond)
		}
		if s.booting() {
			http.Error(w, "demesne: the "+s.specOf().name+" router is still starting; retry shortly",
				http.StatusServiceUnavailable)
			return false
		}
	}
	// The group is the one of the tenant the request actually reaches, not of the path: a
	// model that lives on the small card must not kill a render on the big one.
	if sd := d.tenantByName("sd"); sd != nil && sd.specOf().group == s.specOf().group {
		// If a render is in flight, wait for it to finish -- then say so plainly instead
		// of killing it.
		if n := atomic.LoadInt64(&sd.inflight); n > 0 {
			deadline := time.Now().Add(d.cfg.busyWait)
			for atomic.LoadInt64(&sd.inflight) > 0 && time.Now().Before(deadline) {
				time.Sleep(500 * time.Millisecond)
			}
			if atomic.LoadInt64(&sd.inflight) > 0 {
				http.Error(w, "demesne: an image generation is in progress on the card; retry shortly",
					http.StatusServiceUnavailable)
				return false
			}
		}
		if up, _ := sd.isUp(); up {
			d.evict(sd, stopChat, "a chat request wants the card")
		}
	}
	// Sibling MODEL tenants share the card too. A request may take it from any other tenant in
	// the group -- least recently used first -- but never from the tenant it is being routed
	// to, and never from a resident one: resident is the operator saying keep it.
	for _, other := range d.cardSiblings(s) {
		d.evict(other, stopChat, "a request for "+s.specOf().name+" wants the card")
	}
	return true
}

// defaultTenant is the engine that answers when no tenant declares or advertises the model, and
// the one whose own /v1/models is used when no router answered. It is the llama tenant when the
// estate has one, so no caller has to know the name.
func (d *demesne) defaultTenant() *tenant {
	if s := d.tenantByName("llama"); s != nil {
		return s
	}
	for _, s := range d.tenants {
		if s.specOf().router && s.specOf().routed {
			return s
		}
	}
	return nil
}

// cardSiblings lists the tenants that hold part of the same card and may be asked to give it
// back, least recently used first. The requester is never in the list.
func (d *demesne) cardSiblings(s *tenant) []*tenant {
	group := s.specOf().group
	if group == "" {
		return nil
	}
	var out []*tenant
	for _, other := range d.tenants {
		spec := other.specOf()
		if other == s || spec.group != group || !spec.vram || spec.policy == policyResident {
			continue
		}
		// Only a tenant demesne owns can be told to give the card back; an external engine's
		// VRAM is not ours to reclaim, and pretending otherwise would lie in /status.
		if !spec.own() {
			continue
		}
		if up, _ := other.isUp(); !up {
			continue
		}
		out = append(out, other)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].lastUsed().Before(out[j].lastUsed())
	})
	return out
}

// ------------------------------------------------------------------- /status

type tenantStatus struct {
	Name       string  `json:"name"`
	Model      string  `json:"model,omitempty"`
	Alias      string  `json:"alias,omitempty"`
	Kind       string  `json:"kind"`
	Device     string  `json:"device"`
	State      string  `json:"state"`
	Listen     string  `json:"listen"`
	ListenPort int     `json:"port"`
	Group      string  `json:"group,omitempty"`
	Policy     string  `json:"policy"`
	VRAMMB     int     `json:"vram_mb,omitempty"`
	NCtx       int64   `json:"n_ctx,omitempty"`
	NParams    int64   `json:"n_params,omitempty"`
	SizeBytes  int64   `json:"size_bytes,omitempty"`
	Quant      string  `json:"quant,omitempty"`
	ReloadS    float64 `json:"reload_s,omitempty"`
	Idle       string  `json:"idle,omitempty"`
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
	// LastTransition is why this tenant's child is not running: idle, chat-claims-card,
	// operator or failed. Absent while it is running, because then there is nothing to
	// explain -- it is cleared on readiness for the same reason last_error is.
	LastTransition stopReason    `json:"last_transition,omitempty"`
	Cgroup         *cgroupStatus `json:"cgroup,omitempty"`
}

func (d *demesne) tenantStatuses() []tenantStatus {
	out := make([]tenantStatus, 0, len(d.tenants))
	for _, s := range d.tenants {
		out = append(out, tenantStatusOf(s))
	}
	return out
}

// tenantStatusOf reads one tenant without the transition lock, so a status poll is never
// stuck behind an eight-minute cold load.
// firstAlias is the name a caller is meant to use when it differs from the model id the engine
// reports: an alias is what a person types, the model id is what llama-server calls itself.
func firstModelOrAlias(spec tenantSpec) string {
	if a := firstAlias(spec); a != "" {
		return a
	}
	if m := firstModel(spec); m != "" {
		return m
	}
	return spec.name
}

func firstAlias(spec tenantSpec) string {
	if len(spec.aliases) > 0 {
		return spec.aliases[0]
	}
	return ""
}

func firstModel(spec tenantSpec) string {
	if len(spec.models) > 0 {
		return spec.models[0]
	}
	return ""
}

func tenantState(spec tenantSpec, t *tenant) string {
	if t.up {
		if t.readyAt.IsZero() {
			return "loading"
		}
		return "ready"
	}
	if downReason(t.stopped, t.up) == stopFailed {
		return "failed"
	}
	return "idle"
}

// configuredCtx is the context the tenant is started with, which is what a meter shows; the
// architecture's training context is only the fallback for a tenant that does not say.
func configuredCtx(spec tenantSpec) int64 {
	for i, a := range spec.args {
		switch {
		case a == "-c" || a == "--ctx-size" || a == "-ctx-size":
			if i+1 < len(spec.args) {
				if n, err := strconv.ParseInt(spec.args[i+1], 10, 64); err == nil {
					return n
				}
			}
		case strings.HasPrefix(a, "--ctx-size="):
			if n, err := strconv.ParseInt(strings.TrimPrefix(a, "--ctx-size="), 10, 64); err == nil {
				return n
			}
		}
	}
	return ggufOf(modelPath(spec)).NCtx
}

func reloadCostSeconds(spec tenantSpec) float64 {
	n := weightsBytes(spec)
	if n == 0 {
		return 0
	}
	return float64(n) / (defaultReadMBs * 1e6)
}

func tenantStatusOf(s *tenant) tenantStatus {
	s.mu.Lock()
	spec := s.specOf()
	st := tenantStatus{
		Name: spec.name, Model: firstModel(spec), Alias: firstAlias(spec),
		Kind: spec.kind.String(), Device: cardLabel(spec),
		State: tenantState(spec, s), Listen: spec.url, ListenPort: s.port(),
		Group: spec.group, Policy: spec.policy.String(), Owned: spec.own(),
		VRAMMB: spec.vramMB, ReloadS: reloadCostSeconds(spec), Idle: idleString(spec),
		NCtx: configuredCtx(spec), NParams: ggufOf(modelPath(spec)).NParams,
		SizeBytes: ggufOf(modelPath(spec)).SizeByte, Quant: ggufOf(modelPath(spec)).Quant,
		Up: s.up, Booting: s.up && s.readyAt.IsZero(), PID: s.pid,
		InFlight: atomic.LoadInt64(&s.inflight), Restarts: s.restarts,
		LastError: s.lastErr, LastTransition: downReason(s.stopped, s.up),
	}
	if s.specOf().idle > 0 {
		st.IdleSecond = s.specOf().idle.Seconds()
	}
	if s.up && !s.readyAt.IsZero() {
		st.UptimeSecs = time.Since(s.readyAt).Seconds()
	}
	if s.specOf().cgroup.set() {
		st.Cgroup = &cgroupStatus{
			Leaf:   s.specOf().cgroup.leaf,
			High:   s.specOf().cgroup.high,
			Max:    s.specOf().cgroup.max,
			Swap:   s.specOf().cgroup.swapHigh,
			Joined: s.cgJoined,
			At:     s.cgAt,
			Err:    s.cgErr,
		}
	}
	foreign := s.foreign
	s.mu.Unlock()

	// An external engine is someone else's child, so "up" has to be asked of its port
	// rather than of our process table -- and it is never a foreign holder, because the
	// port is its own by contract. This matters during the migration: most engines are
	// external while demesne takes them over one at a time.
	if s.specOf().policy == policyExternal {
		st.Up = portAliveWithin(s.specOf().url, 150*time.Millisecond)
		st.Booting = false
		return st
	}

	// A port answered while the tenant is down means someone else owns it. Bounded
	// short: /status is polled, and it must never be the slowest thing on the box.
	if !st.Up && portAliveWithin(s.specOf().url, 150*time.Millisecond) {
		foreign = true
	}
	if foreign {
		st.Foreign = "a process demesne does not own"
	}
	return st
}

func (d *demesne) status(w http.ResponseWriter) {
	sd := d.tenantByName("sd")
	up, pid := false, 0
	inflight := int64(0)
	if sd != nil {
		up, pid = sd.isUp()
		inflight = atomic.LoadInt64(&sd.inflight)
	}
	used, _ := vramUsed()
	statuses := d.tenantStatuses()
	out := map[string]any{
		// The estate, as tenants.
		"version":         version,
		"listen":          d.cfg.listen,
		"socket":          d.cfg.socket,
		"vram_used_gib":   float64(used) / (1 << 30),
		"media_in_flight": inflight,
		"llm_health":      d.llm.snapshot(),
		"tenants":         statuses,
		"slots":           statuses,
		"loads":           d.loads.snapshot(),

		// stadi's field names, kept because DSH and the habit of a year read them.
		"tenant":         tenantName(up),
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

func (d *demesne) handleSlotsPath(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/slots" {
		http.Error(w, "demesne: /slots is llama's own, and demesne no longer proxies it: "+
			"use /tenants/<name>/status (per engine) or /status (the estate)", http.StatusNotFound)
		return
	}
	slotsDeprecation.Do(func() {
		log.Printf("deprecated: /slots/<name> is demesne's operator path and now answers as /tenants/<name>; " +
			"/slots alone is llama's and is not proxied")
	})
	d.handleTenantControl(w, r)
}

// tenantName is stadi's one-bit answer to "who holds the card", kept as-is: it is what
// the old /status published and nothing gained by renaming it.
func tenantName(sdUp bool) string {
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
	Group   string       `json:"group"`
	Holders []string     `json:"holders"`
	SinceMS int64        `json:"since_ms"`
	Tenants []cardTenant `json:"tenants"`
}

type cardTenant struct {
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
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(d.cardDocument(group))
}

// cardDocument is the card answer itself, so the HTTP endpoint and the typed control plane
// cannot drift apart: one builds it, the other wraps it.
func (d *demesne) cardDocument(group string) estateCard {
	out := estateCard{Group: group, Holders: []string{}, Tenants: []cardTenant{}}
	for _, s := range d.tenants {
		if s.specOf().group != group {
			continue
		}
		st := tenantStatusOf(s)
		state := "stopped"
		switch {
		case st.Up && st.Booting:
			state = "loading"
		case st.Up:
			state = "running"
		}
		out.Tenants = append(out.Tenants, cardTenant{Name: s.specOf().name, State: state})
		if state != "running" || !d.holdsTheCard(s) {
			continue
		}
		out.Holders = append(out.Holders, s.specOf().name)
		if since := s.readySince().UnixMilli(); since > 0 && (out.SinceMS == 0 || since < out.SinceMS) {
			out.SinceMS = since
		}
	}
	return out
}

// downReason makes last_transition a total function for a tenant that is down: a tenant that has
// never run is down for want of a request, which is the same situation the idle timer produces,
// so it says "idle" rather than leaving a client to interpret an absent field as an unknown
// state. While the tenant is up the field is absent -- there is nothing to explain.
func downReason(reason stopReason, up bool) stopReason {
	if up || reason != "" {
		return reason
	}
	return stopIdle
}

// holdsTheCard separates "the engine is up" from "the card is busy". They are the same thing
// for the media child (up means its weights are in memory) and different for a router: idle,
// it holds a port and about 28 MiB, and a client told otherwise would refuse to render on a
// card that is free.
//
// The evidence, in order: a request in flight (it is loading or answering), then the router's
// own model status, then -- for a build that publishes none -- the process itself, which is
// the coarse answer but never the wrong way round.
func (d *demesne) holdsTheCard(s *tenant) bool {
	if !s.specOf().router {
		return true
	}
	if atomic.LoadInt64(&s.inflight) > 0 {
		return true
	}
	if n, known := d.loadedCount(s.specOf().name); known {
		return n > 0
	}
	return true
}

// ------------------------------------------------------------------ /release

func (d *demesne) handleRelease(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("tenant")
	if name != "" && d.tenantByName(name) == nil {
		http.Error(w, "demesne: no such tenant: "+name, http.StatusNotFound)
		return
	}
	released := []string{}
	for _, s := range d.tenants {
		if name != "" && s.specOf().name != name {
			continue
		}
		if s.specOf().policy != policyOnDemand {
			continue
		}
		if up, _ := s.isUp(); !up {
			continue
		}
		d.evict(s, stopOperator, "operator release")
		released = append(released, s.specOf().name)
	}
	if len(released) == 0 {
		fmt.Fprintln(w, "released nothing")
		return
	}
	fmt.Fprintf(w, "released %s\n", strings.Join(released, " "))
}

// handleTenantControl is the per-engine operator interface: /tenants/<name>{,/status,
// /load,/stop}. It replaces "rc-service --user llama restart" as the way to touch one
// engine, because rc-service can no longer see a child demesne forked.
var slotsDeprecation sync.Once

func (d *demesne) handleTenantControl(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/tenants/")
	if rest == r.URL.Path {
		rest = strings.TrimPrefix(r.URL.Path, "/slots/")
	}
	name, action := rest, ""
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		name, action = rest[:i], rest[i+1:]
	}
	s := d.tenantByName(name)
	if s == nil {
		http.Error(w, "demesne: no such tenant: "+name, http.StatusNotFound)
		return
	}
	switch action {
	case "", "status":
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(tenantStatusOf(s))
	case "load", "start":
		if r.Method != http.MethodPost {
			http.Error(w, "demesne: POST required", http.StatusMethodNotAllowed)
			return
		}
		if err := d.ensure(s); err != nil {
			http.Error(w, "demesne: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, "%s loaded\n", s.specOf().name)
	case "stop", "release":
		if r.Method != http.MethodPost {
			http.Error(w, "demesne: POST required", http.StatusMethodNotAllowed)
			return
		}
		d.evict(s, stopOperator, "operator stop")
		fmt.Fprintf(w, "%s stopped\n", s.specOf().name)
	case "reload":
		if r.Method != http.MethodPost {
			http.Error(w, "demesne: POST required", http.StatusMethodNotAllowed)
			return
		}
		n, err := d.reloadPreset(s)
		if err != nil {
			http.Error(w, "demesne: "+err.Error(), http.StatusConflict)
			return
		}
		fmt.Fprintf(w, "%s: preset reloaded, %d models\n", s.specOf().name, n)
	case "reclaim":
		if r.Method != http.MethodPost {
			http.Error(w, "demesne: POST required", http.StatusMethodNotAllowed)
			return
		}
		killed, err := d.reclaimTenant(s)
		if err != nil {
			http.Error(w, "demesne: "+err.Error(), http.StatusConflict)
			return
		}
		if !killed {
			fmt.Fprintf(w, "%s: nothing was holding its port\n", s.specOf().name)
			return
		}
		fmt.Fprintf(w, "%s: port taken back\n", s.specOf().name)
	default:
		http.Error(w, "demesne: try /tenants/<name>, /load or /stop", http.StatusNotFound)
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
	if err := cfg.validate(d.tenants); err != nil {
		log.Fatal(err)
	}

	log.Printf("listening on %s | llama %s | sd %s | whisper %s | llamb %s | idle %s | vram %s",
		cfg.listen, cfg.llama.url, cfg.sd.url, cfg.whisper.url, cfg.llamb.url, cfg.idle, vramString())
	if cfg.socket != "" {
		log.Printf("the same API is also on unix:%s (mode 0600)", cfg.socket)
	}
	for _, t := range sortedTenantPtrs(d.tenants) {
		log.Print(tenantLogLine(t.specOf()))
	}

	// A port answering before demesne started the engine is either a leftover or
	// another supervisor; either way it is the collision this program refuses to race.
	for _, s := range d.tenants {
		if s.specOf().url == "" || s.specOf().policy == policyExternal {
			continue
		}
		if portAlive(s.specOf().url) {
			log.Printf("warning: %s is already answering on %s; demesne did not start it",
				s.specOf().name, s.specOf().url)
		}
	}

	d.prepareCgroupRoot()
	for _, s := range d.tenants {
		d.prepareCgroup(s)
	}

	// supervise-daemon stops us with SIGTERM. Because every child sits in its own
	// process group it would NOT die with us, and an orphaned sd-server is exactly the
	// 12.5 GiB hostage this program exists to prevent -- so take them all down first.
	stop := make(chan os.Signal, 1)
	go func() {
		hup := make(chan os.Signal, 1)
		signal.Notify(hup, syscall.SIGHUP)
		for range hup {
			rep, err := d.reload()
			if err != nil {
				log.Printf("reload: %v (the estate is unchanged)", err)
				continue
			}
			log.Printf("reload: %s", rep.String())
		}
	}()
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

	for _, s := range d.tenants {
		if s.specOf().policy == policyResident || s.specOf().policy == policyKeep {
			go d.supervise(s)
		}
	}

	go d.maintainCgroups()

	// The index is built before the listener opens, so the very first request routes by
	// model name; the ticker keeps it honest afterwards.
	d.models = newModelIndex()
	d.refreshModels()
	go d.followModels()
	d.loads.onChange = d.loadChanged
	go d.followLoads()
	go d.publishSnapshots()

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
// answers both, so the OpenAI dialect, SSE streaming and the tenant control endpoints are
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
