// tenant.go -- the estate: one entry per engine, and the rules about who may hold
// what.
//
// A tenant is one struct with one lifecycle, because the four engines differ only in
// data:
//
//	tenant     port   group  policy     routed  router  why
//	llama    55556  gpu    resident   yes     yes     the router; keeps a resident rung
//	sd       55558  gpu    on demand  yes     no      cannot unload, so it must be evicted
//	whisper  55559  --     on demand  yes     no      CPU-only: never contends for the card
//	llamb    55557  aux    resident   yes     yes     second card; its models are routed too
//
// Only the routers answer /v1/models, and that answer is what decides which engine serves
// a request that names a model -- demesne sits on 55555, the number every caller already
// means, so the name has to choose. The numbers are the engines' own; the estate's is the
// one in front.
//
// Two scarce resources, therefore two rules:
//
//	the card   one holder per exclusivity GROUP. A resident holder (the router) is
//	           displaced by asking it to unload its models; an on-demand holder (sd)
//	           is displaced by stopping it, because that is its only way to give the
//	           weights back. A tenant that holds neither (whisper) has no group and is
//	           never evicted by another tenant's request. The rule is applied to the group
//	           of the tenant the request actually goes to, so a model served by the small
//	           card does not evict a render from the big one.
//	a port     a tenant may only spawn when its own port is free, and a port already
//	           answered by a process demesne does not own is a REFUSAL, not a race.
//	           That is the durable half of the old 55557 collision: sd and whisper both
//	           claimed it, and nothing enforced exclusivity.
//
// Policies: an engine OpenRC still supervises is policyExternal (demesne routes to it
// and never spawns or kills it); an engine demesne owns is either policyOnDemand
// (spawned by a request, evicted when idle or when a group peer wants the card) or
// policyResident (spawned at startup, restarted with backoff, never evicted).
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// policy is who owns the child's lifetime.
type policy int

const (
	policyExternal policy = iota // supervised elsewhere (OpenRC); route only
	policyOnDemand               // spawn for a request, evict when idle
	policyResident               // spawn at startup, restart with backoff, never evicted
	policyKeep                   // spawn at startup like a resident, but give the card back when
	// another tenant needs it: no idle timer, eviction only for fitting
)

func (p policy) String() string {
	switch p {
	case policyOnDemand:
		return "on-demand"
	case policyResident:
		return "resident"
	case policyKeep:
		return "keep"
	default:
		return "external"
	}
}

// memLimit is one tenant's memory fence. A cgroup v2 leaf under a delegated subtree;
// empty fields are left alone. The zero value means "no fence".
//
// These live in the tenant spec rather than in an init script because the engines are
// demesne's children now: `rc_cgroup_settings` never worked for a --user service
// anyway (openrc-run.sh:60 does not source rc-cgroup.sh under RC_USER_SERVICES), which
// is why every init script carried forty lines of start_post() to do it by hand.
// stopReason is why a tenant's child is not running. A closed vocabulary because a client
// renders it: "idle" and "operator" are ordinary, "chat-claims-card" is the one a reader wants
// explained, and "failed" is the only one that is a fault. Empty means "running", or "never
// stopped since it last became ready" -- the field is cleared on readiness for the same reason
// last_error is.
type stopReason string

const (
	stopIdle     stopReason = "idle"
	stopChat     stopReason = "chat-claims-card"
	stopOperator stopReason = "operator"
	stopFailed   stopReason = "failed"
)

// cgroupLeaf is where a tenant's fence sits under the estate's cgroup root. The two routers
// share a parent -- they are the two halves of one job -- and a parent that holds no
// processes is also the obvious place for a limit that would cover both. Everything else
// is flat.
func cgroupLeaf(tenant string) string {
	switch tenant {
	case "llama":
		return "llama/llm"
	case "llamb":
		return "llama/aux"
	}
	return tenant
}

// makeLeaf builds one tenant's leaf inside the estate's subtree, without privilege.
//
// This works because the subtree is delegated to this user: cgroup-prepare chowns the
// directories it makes, and files cgroupfs creates for a cgroup we make are owned by us, so
// a leaf we create here is a leaf we can write limits into. It fails on a subtree owned by
// root -- a first start, or a box whose /sys/fs/cgroup was rebuilt -- and the caller falls
// back to the helper for exactly that case.
func makeLeaf(m memLimit) error {
	if err := os.MkdirAll(m.leaf, 0o755); err != nil {
		return err
	}
	// A controller only appears in a child once it is enabled in the parent, and the
	// parent's subtree_control is idempotent to write.
	parent := filepath.Join(filepath.Dir(m.leaf), "cgroup.subtree_control")
	if _, err := os.Stat(filepath.Join(m.leaf, "memory.high")); err != nil {
		if err := os.WriteFile(parent, []byte("+memory +pids"), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// cgroupStatus is the fence as /status reports it: what is set, whether the child is inside
// it, and why not when it is not. "Maintained" is a claim, so it is published with a
// timestamp rather than assumed.
type cgroupStatus struct {
	Leaf   string    `json:"leaf"`
	High   string    `json:"high,omitempty"`
	Max    string    `json:"max,omitempty"`
	Swap   string    `json:"swap_high,omitempty"`
	Joined bool      `json:"joined"`
	At     time.Time `json:"applied_at,omitempty"`
	Err    string    `json:"error,omitempty"`
}

type memLimit struct {
	leaf     string // /sys/fs/cgroup/demesne/<tenant>
	high     string
	max      string
	swapHigh string
}

func (m memLimit) set() bool {
	return m.leaf != "" && (m.high != "" || m.max != "" || m.swapHigh != "")
}

// tenantSpec is the whole of a tenant's configuration. Deliberately plain data: the
// lifecycle below is generic, and a new engine is a new literal in buildTenants.
type tenantSpec struct {
	name string
	url  string // base URL; also the port the child must bind

	bin  string   // empty: the tenant owns no child (policyExternal)
	args []string // the child's whole command line
	env  []string // extra KEY=VALUE for the child, appended to demesne's own
	// models seeds the routing index for a tenant that cannot answer yet: an on-demand or
	// external engine that is down has no /v1/models to read, so without a seed its names are
	// unknown after every demesne restart and a request for them falls through to the router.
	// It is a seed, not a source of truth: the first answer from the engine replaces it.
	models []string
	// dir is the child's working directory. A router's models.ini names its weights
	// relative to it, so this is load-bearing, not tidiness.
	dir string

	kind         tenantKind
	aliases      []string
	boot         bool
	vramMB       int
	readMBs      float64
	weightsBytes int64

	group  string // exclusivity group ("" = contends for nothing)
	vram   bool   // the tenant's processes hold part of the card
	policy policy
	idle   time.Duration // evict an on-demand child after this long with no request

	routed bool // demesne may route a request to it
	// router: an engine that advertises models on /v1/models, and whose answer decides
	// which tenant serves a request that names a model. sd and whisper are routed but are
	// not routers: what they serve is chosen by the path, not by a model name.
	router bool
	// fallback: proxy to it even when demesne does not own it. True for an engine
	// still supervised by OpenRC (llama), false for one demesne is meant to own and
	// simply has not been configured yet (whisper) -- which must answer 503 rather
	// than fall through to the router.
	fallback bool

	cgroup memLimit
}

// tenant is the spec plus the live state. mu guards the state and is held only for
// short reads and writes; a transition (which may take minutes: a cold sd load is
// ~8 min) holds demesne.transition instead, so /status is never blocked by a load.
type tenant struct {
	// spec is written once at construction and by a reload, and read through specOf().
	// The lock is separate from mu on purpose: specOf() is called from inside mu-held
	// regions, and one lock for both would either deadlock or force every reader to
	// know which region it is in.
	spec   tenantSpec
	specMu sync.RWMutex

	mu       sync.Mutex
	child    *exec.Cmd
	up       bool
	pid      int
	started  time.Time
	readyAt  time.Time
	used     time.Time // last request routed to this tenant; drives LRU eviction
	stopping bool      // the exit was ordered, so do not count it as a crash
	foreign  bool      // the port answers but the process is not ours
	lastErr  string
	stopped  stopReason // why the child last stopped; cleared when it is ready again
	restarts int
	crash    int // consecutive unexpected failures; drives the backoff
	diedAt   time.Time
	timer    *time.Timer

	inflight int64 // requests in flight (atomic): keeps the idle timer off

	owner       *demesne
	requests    atomic.Int64
	errors      atomic.Int64
	active      atomic.Int64
	lastLatency atomic.Int64

	supervised bool      // a supervisor goroutine is responsible for this tenant
	cgJoined   bool      // the current child was moved into its leaf
	cgAt       time.Time // when the fence (or the join) last succeeded
	cgErr      string

	// gone is closed when the child this tenant last started has exited. One channel per
	// child, not one per tenant: with a single tenant-wide channel a token left behind by a
	// failed spawn would make the supervisor believe the NEXT child had died, and it
	// would start a duplicate beside a live engine.
	gone chan struct{}
}

// ----------------------------------------------------------------- lifecycle

func (s *tenant) hostPort() string { return uriHostPort(s.specOf().url) }

// port is the numeric port, for messages and duplicate-port checks.
func (s *tenant) port() int { return uriPort(s.specOf().url) }

func (s *tenant) isUp() (bool, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.up, s.pid
}

// booting is "started, not yet answering". A resident router spends a cold load in
// this state, and a chat request that arrives then should wait rather than take a
// 502 from the proxy.
func (s *tenant) booting() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.up && s.readyAt.IsZero()
}

func (s *tenant) isSupervised() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.supervised
}

func (s *tenant) setSupervised(v bool) {
	s.mu.Lock()
	s.supervised = v
	s.mu.Unlock()
}

// specOf is the tenant's configuration as one consistent value. A reload replaces the whole
// struct, so a copy is a snapshot and a reader never sees half an apply.
func (s *tenant) specOf() tenantSpec {
	s.specMu.RLock()
	defer s.specMu.RUnlock()
	return s.spec
}

func (s *tenant) setSpec(spec tenantSpec) {
	s.specMu.Lock()
	s.spec = spec
	s.specMu.Unlock()
}

// readySince is when this tenant last became ready, or the zero time for an engine we
// never started: an external engine's readiness is someone else's event, and the only
// thing we can honestly report about it is that its port answers now.
func (s *tenant) readySince() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readyAt
}

// backoffLeft implements the crash backoff, which the old sd lifecycle did not have:
// a crash was logged, marked down, and the very next request retried immediately --
// each attempt able to pay a 15-minute cold load. Now a tenant that keeps dying is
// spaced out, and the message says so instead of hanging.
func (s *tenant) backoffLeft() (time.Duration, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.crash == 0 || s.diedAt.IsZero() {
		return 0, 0
	}
	d := backoff(s.crash)
	if elapsed := time.Since(s.diedAt); elapsed < d {
		return d - elapsed, s.crash
	}
	return 0, 0
}

func (s *tenant) noteFailure(err error) {
	s.mu.Lock()
	s.crash++
	s.diedAt = time.Now()
	s.lastErr = err.Error()
	s.mu.Unlock()
}

func (s *tenant) noteReady() {
	d := s.owner
	if d != nil {
		d.recordTransition(s.specOf().name, "start", "ready", "")
	}
	s.mu.Lock()
	s.crash, s.lastErr, s.foreign, s.stopped = 0, "", false, ""
	s.mu.Unlock()
}

// armIdle (re)arms the idle timer. It must be called at the END of a request, not the
// start: a cold render takes ~8-10 minutes, so a five-minute timer armed before it
// would kill the child mid-load. The in-flight count is what tells the timer a
// request is still running.
func (s *tenant) lastUsed() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used
}

func (s *tenant) noteUse() {
	s.mu.Lock()
	s.used = time.Now()
	s.mu.Unlock()
}

func (s *tenant) armIdle(d *demesne) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.armIdleLocked(d)
}

func (s *tenant) armIdleLocked(d *demesne) {
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	// A resident tenant is the operator saying "keep this up": it is not idle-evicted, and a
	// timer here was evicting the aux embedder every five minutes.
	if s.specOf().policy == policyResident || s.specOf().policy == policyKeep || s.specOf().idle <= 0 {
		return
	}
	s.timer = time.AfterFunc(s.specOf().idle, func() {
		if atomic.LoadInt64(&s.inflight) > 0 {
			// Still rendering: start the clock again rather than killing it.
			s.armIdle(d)
			return
		}
		d.evict(s, stopIdle, fmt.Sprintf("no request for %s", s.specOf().idle))
	})
}

// -------------------------------------------------------------- transitions
//
// demesne.transition serialises every state change on the estate: only one tenant may
// cross the card at a time. It is never held across a proxy, and /status never takes
// it (it reads each tenant's mu), so an eight-minute cold load cannot wedge the status
// surface that exists to explain the eight-minute cold load.

func (d *demesne) ensure(s *tenant) error {
	// Before taking the transition lock, give a held port one chance to be taken back -- see
	// reclaim.go. It can wait for a process to die, and no other transition should queue
	// behind that.
	if up, _ := s.isUp(); !up && portAlive(s.specOf().url) {
		_, _ = d.reclaimTenant(s)
	}
	d.transition.Lock()
	defer d.transition.Unlock()
	return d.ensureLocked(s)
}

func (d *demesne) evict(s *tenant, reason stopReason, detail string) {
	d.transition.Lock()
	defer d.transition.Unlock()
	d.evictLocked(s, reason, detail)
}

func (d *demesne) ensureLocked(s *tenant) error {
	if up, _ := s.isUp(); up && portAlive(s.specOf().url) {
		s.armIdle(d)
		return nil
	}
	if portAlive(s.specOf().url) {
		s.mu.Lock()
		s.foreign = true
		s.mu.Unlock()
		return fmt.Errorf("%s is already served by a process demesne does not own "+
			"(find it with: %s; stop it, then retry; see /status: port_held_elsewhere)",
			s.specOf().url, s.endpointHint())
	}
	if !s.specOf().own() {
		return fmt.Errorf("%s is not configured: give it a command line", s.specOf().name)
	}
	if wait, n := s.backoffLeft(); wait > 0 {
		return fmt.Errorf("%s has failed %d time(s) in a row; retrying in %s",
			s.specOf().name, n, wait.Round(time.Second))
	}
	if err := d.clearGroupLocked(s); err != nil {
		return err
	}
	model := firstModel(s.specOf())
	if model != "" {
		d.loads.noteLoading(s.specOf().name, model)
	}
	err := d.spawnLocked(s)
	if err != nil {
		if used, uerr := vramUsed(); uerr == nil {
			log.Printf("%s: spawn failed (%v); vram used %.2f GiB at the failure "+
				"(the child's own error is in its .err log)", s.specOf().name, err,
				float64(used)/(1<<30))
		}
	}
	if model != "" {
		if err != nil {
			d.loads.notePhase(s.specOf().name, model, loadFailed, err)
		} else {
			d.loads.notePhase(s.specOf().name, model, loadReady, nil)
		}
	}
	return err
}

func (s tenantSpec) own() bool { return s.bin != "" && s.policy != policyExternal }

// clearGroupLocked makes room for s: every on-demand peer in the same group is
// stopped, and if s wants the card the router is asked to unload. The order matters --
// stop the peer, wait for the VRAM to actually return, and only then start loading --
// because "the process exited" and "the driver freed the memory" are different
// moments and only the second one makes a load succeed.
func (d *demesne) clearGroupLocked(s *tenant) error {
	if s.specOf().group == "" {
		return nil
	}
	for _, other := range d.tenants {
		if other == s || other.specOf().group != s.specOf().group {
			continue
		}
		if other.specOf().policy == policyResident {
			continue
		}
		if up, _ := other.isUp(); up {
			d.evictLocked(other, stopChat, s.specOf().name+" wants the "+s.specOf().group)
		}
	}
	if !s.specOf().vram {
		return nil
	}
	// Nothing to ask a router to unload any more: a tenant's model is its own process, and the
	// peers that could be in the way were stopped above. What is left is arithmetic, and a
	// declared footprint is the only honest basis for it -- without one, try and let the child
	// say what it could not allocate.
	// The computed footprint is published and is the right shape, but it is not yet calibrated
	// against a measured load (it reads 36 GiB for a 27B that occupies 18), so admission still
	// trusts the declared number: a gate that refuses working rungs is worse than no gate.
	need := uint64(0)
	if specMB := s.specOf().vramMB; specMB > 0 {
		need = uint64(specMB) << 20
	}
	if need == 0 {
		return nil
	}
	used, err := vramUsed()
	if err != nil {
		return nil
	}
	total := vramTotal()
	deadline := time.Now().Add(5 * time.Second)
	for used+need > total && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
		if u, err := vramUsed(); err == nil {
			used = u
		}
	}
	if used+need > total {
		return fmt.Errorf("%s needs %s but only %s of %s is free (see /status: tenants[])",
			s.specOf().name, gib(need), gib(total-used), gib(total))
	}
	return nil
}

func gib(n uint64) string { return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30)) }

func (d *demesne) spawnLocked(s *tenant) error {
	if d.closing.Load() {
		return errors.New("demesne is shutting down")
	}
	// A port already answered by a process demesne does not own is a REFUSAL, not a race:
	// spawning anyway gets a child that cannot bind, dies with a bare "exit status 1", and
	// takes the supervisor into a crash loop that reads as the engine's fault. The orphan
	// that did this on 2026-11-07 was 1 day 19 hours old and invisible in every log.
	if up, _ := s.isUp(); !up && portAliveWithin(s.specOf().url, 250*time.Millisecond) {
		// One chance to take it back, and only on evidence: see reclaim.go. A holder that
		// cannot be proven ours is left alone, loudly.
		killed, rerr := d.reclaimTenant(s)
		if !killed {
			reason := "its port is held by a process demesne cannot prove it owns"
			if rerr != nil {
				reason = rerr.Error()
			}
			s.mu.Lock()
			s.foreign = true
			s.lastErr = reason
			s.mu.Unlock()
			return fmt.Errorf("%s; refusing to spawn a second one (see /status: port_held_elsewhere)", reason)
		}
	}
	if err := os.MkdirAll(d.cfg.logDir, 0o755); err != nil {
		return fmt.Errorf("log dir: %w", err)
	}
	out, err := openLog(d.cfg.logDir, s.specOf().name+".log")
	if err != nil {
		return fmt.Errorf("open %s stdout: %w", s.specOf().name, err)
	}
	errf, err := openLog(d.cfg.logDir, s.specOf().name+".err")
	if err != nil {
		out.Close()
		return fmt.Errorf("open %s stderr: %w", s.specOf().name, err)
	}

	if path := unixPath(s.specOf().url); path != "" {
		if err := ensureSocketDir(path); err != nil {
			return fmt.Errorf("%s: %w", s.specOf().name, err)
		}
		if fi, err := os.Lstat(path); err == nil {
			if fi.Mode()&os.ModeSocket == 0 {
				return fmt.Errorf("%s exists and is not a socket; refusing to clobber it", path)
			}
			if !portAliveWithin(s.specOf().url, 150*time.Millisecond) {
				// A file left by a SIGKILL or by a clean stop: the installed llama-server does
				// not unlink on bind or on exit, so the launcher is the single owner of this
				// file. ENOENT is success -- a concurrent cleanup must not fail a spawn.
				_ = os.Remove(path)
			} else {
				return fmt.Errorf("%s is already served by another process", path)
			}
		}
	}
	cmd := exec.Command(s.specOf().bin, s.specOf().args...)
	// argv[0] is the label a person reads in `ps` and in a top's command column; the executable
	// Go runs is cmd.Path and is untouched, so nothing that finds a child by its file name
	// changes. comm (the NAME column) is a separate thing and stays llama-server.
	if label := firstModelOrAlias(s.specOf()); label != "" {
		cmd.Args[0] = label
	}
	cmd.Dir = s.specOf().dir
	cmd.Stdout, cmd.Stderr = out, errf
	cmd.Env = append(os.Environ(), s.specOf().env...)
	// One process group, so stopping the child also stops anything it forked. The
	// cost is that it does NOT die with demesne: an orphaned sd-server is ~12.5 GiB
	// nothing owns any more, so demesne takes every child down on the way out.
	// Pdeathsig is the half a SIGTERM handler cannot cover: if demesne is SIGKILLed -- by the
	// sandbox that launched it, by a wedged supervisor, by anything -- the kernel kills the
	// child too. Without it the child sits in its own process group with nobody left to stop
	// it, which is exactly how a 1d19h aux router came to hold a port that the next llamb
	// needed. The cost is that a child also dies if the forking thread dies; Go parks its
	// threads rather than destroying them, and an engine that dies with its owner is worth
	// more here than one that outlives it.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	if s.specOf().own() {
		log.Printf("starting %s: %s %s", s.specOf().name, s.specOf().bin, strings.Join(s.specOf().args, " "))
	}
	if err := cmd.Start(); err != nil {
		out.Close()
		errf.Close()
		s.noteFailure(err)
		return fmt.Errorf("start %s: %w", s.specOf().name, err)
	}

	s.mu.Lock()
	s.child, s.pid, s.up = cmd, cmd.Process.Pid, true
	s.started, s.readyAt, s.foreign = time.Now(), time.Time{}, false
	s.mu.Unlock()
	gone := make(chan struct{})
	s.mu.Lock()
	s.gone = gone
	s.mu.Unlock()
	go d.reap(s, cmd, out, errf, gone)
	d.joinCgroup(s, cmd.Process.Pid)

	if err := d.waitReady(s, d.cfg.loadWait); err != nil {
		d.evictLocked(s, stopFailed, fmt.Sprintf("%s never answered on %s", s.specOf().name, s.specOf().url))
		s.noteFailure(err)
		return fmt.Errorf("%s never came up on %s: %w", s.specOf().name, s.specOf().url, err)
	}
	s.mu.Lock()
	s.readyAt = time.Now()
	s.mu.Unlock()
	s.noteReady()
	log.Printf("tenant %s ready on %s (pid %d), vram %s, reload %s, idle %s",
		s.specOf().name, s.specOf().url, cmd.Process.Pid, vramString(), reloadSeconds(s.specOf()), idleString(s.specOf()))
	s.armIdle(d)
	return nil
}

// waitReady bounds the wait for a child's port -- and watches the child while it waits.
// A plain port poll is wrong in two ways: it cannot tell "still loading" from "died on
// the way", so a startup crash would block for the whole load bound (15 minutes for sd)
// before anyone heard about it, and it cannot be interrupted, so a SIGTERM during a cold
// load would leave demesne hanging. Both are checked here.
func (d *demesne) waitReady(s *tenant, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		// Our child first: a port that answers while the child is already dead is somebody
		// else's listener, and taking it as our own readiness is how an orphan is mistaken
		// for a successful start.
		if up, _ := s.isUp(); !up {
			return errors.New("the child exited while starting")
		}
		if portAlive(s.specOf().url) {
			// A single-model llama-server binds its socket BEFORE the weights are ready and
			// answers 503 "Loading model" meanwhile, so a dial alone is not readiness: the
			// first request would be relayed that 503. Ask the engine.
			if len(s.specOf().models) == 0 || d.modelAnswers(s) {
				return nil
			}
		}
		select {
		case <-d.done:
			return errors.New("interrupted by shutdown")
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("timed out after %s", limit)
}

func (d *demesne) modelAnswers(s *tenant) bool {
	client := http.DefaultClient
	base := strings.TrimRight(s.specOf().url, "/")
	if path := unixPath(s.specOf().url); path != "" {
		client = &http.Client{Transport: unixDialer(path)}
		base = "http://unix-socket"
	}
	req, err := http.NewRequest(http.MethodGet, base+"/health", nil)
	if err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return resp.StatusCode < 500
}

// reap notices an exit without waiting for a request to fail.
func (d *demesne) reap(s *tenant, cmd *exec.Cmd, out, errf io.Closer, gone chan struct{}) {
	err := cmd.Wait()
	out.Close()
	errf.Close()

	s.mu.Lock()
	mine := s.child == cmd
	ordered := s.stopping
	if mine {
		s.child, s.pid, s.up = nil, 0, false
		s.readyAt = time.Time{}
		// The fence described a child that no longer exists. /status said "joined" for a
		// tenant with nothing running, which is the same class of lie as a fence that was
		// never applied.
		s.cgJoined = false
		if !ordered {
			s.crash++
			s.diedAt = time.Now()
			s.lastErr = fmt.Sprintf("exited unexpectedly: %v", err)
			s.stopped = stopFailed
		}
		s.stopping = false
	}
	s.mu.Unlock()
	// Always this child's own channel, whether or not the tenant has moved on: the
	// waiter for this process is the only thing on the other end.
	close(gone)
	log.Printf("%s (pid %d) exited: %v", s.specOf().name, cmd.Process.Pid, err)
}

// evictLocked stops the child and waits for the card to come back. SIGTERM the whole
// group, then SIGKILL if it will not go: sd-server frees its weights on exit
// (measured 12.5 GiB -> 0.03 GiB), which is the entire point of stopping it.
func (d *demesne) evictLocked(s *tenant, reason stopReason, why string) {
	s.mu.Lock()
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	pid := s.pid
	if pid == 0 {
		s.child, s.up, s.readyAt, s.stopping = nil, false, time.Time{}, false
		s.mu.Unlock()
		return
	}
	// Recorded only when there was something to stop: an operator calling stop on a tenant
	// that is already down must not erase the failure that put it down.
	s.stopped = reason
	if s.owner != nil {
		s.owner.recordTransition(s.specOf().name, "stop", string(reason), why)
	}
	s.stopping = true
	s.mu.Unlock()

	if why != "" {
		log.Printf("stopping %s (pid %d): %s, vram %s", s.specOf().name, pid, why, vramString())
	}
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if !portAlive(s.specOf().url) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if portAlive(s.specOf().url) {
		log.Printf("%s (pid %d) ignored SIGTERM; killing", s.specOf().name, pid)
		_ = syscall.Kill(-pid, syscall.SIGKILL)
	}

	s.mu.Lock()
	s.child, s.pid, s.up, s.readyAt, s.stopping = nil, 0, false, time.Time{}, false
	s.mu.Unlock()

	if !s.specOf().vram {
		return
	}
	if err := waitVRAMBelow(d.cfg.freeBelow, d.cfg.vramWait); err != nil {
		log.Printf("warning after stopping %s: %v", s.specOf().name, err)
	} else {
		log.Printf("card released, vram %s", vramString())
	}
}

// ---------------------------------------------------------------- supervisor

// supervise is the resident policy: keep the engine alive, forever, with a growing
// delay between attempts. There is exactly ONE actor per restart now, which is the
// fix for the double restart: supervise-daemon used to respawn llama every 5 s while
// recoverLLM() asked rc-service for the same restart after two proxy failures.
func (d *demesne) supervise(s *tenant) {
	delay := d.cfg.restartDelay
	attempt := 0
	for {
		select {
		case <-d.done:
			return
		default:
		}
		if !s.isSupervised() {
			return
		}

		if attempt > 0 {
			// One number for "how many times has this engine been restarted": every
			// spawn after the first, whether the previous one crashed or was killed.
			// Counted BEFORE the spawn, so a supervisor that is starting the engine is
			// not invisible to /status for the duration of a cold load.
			s.mu.Lock()
			s.restarts++
			s.mu.Unlock()
		}
		attempt++

		d.transition.Lock()
		err := d.spawnLocked(s)
		d.transition.Unlock()
		if err != nil {
			if d.closing.Load() {
				return
			}
			log.Printf("supervise: %s did not start: %v (retrying in %s)", s.specOf().name, err, delay)
			if !d.pause(s, delay) {
				return
			}
			delay = nextBackoff(delay, d.cfg.restartDelay)
			continue
		}

		started := time.Now()
		s.mu.Lock()
		gone := s.gone
		s.mu.Unlock()
		select {
		case <-d.done:
			return
		case <-gone:
		}
		// A deliberate stop stays stopped: an operator unload, an idle eviction or a request
		// that took the card is a decision, and a supervisor that undoes it makes the verb a
		// lie. The tenant comes back when a request asks for it, and supervision resumes then.
		s.mu.Lock()
		reason := s.stopped
		s.mu.Unlock()
		if reason == stopOperator || reason == stopIdle || reason == stopChat {
			for {
				if up, _ := s.isUp(); up {
					break
				}
				select {
				case <-d.done:
					return
				case <-time.After(500 * time.Millisecond):
				}
			}
			attempt, delay = 0, d.cfg.restartDelay
			continue
		}
		// A child that lived a while was healthy; start the delay sequence again.
		if time.Since(started) > 60*time.Second {
			delay = d.cfg.restartDelay
		}
		log.Printf("supervise: %s is gone; restarting in %s", s.specOf().name, delay)
		if !d.pause(s, delay) {
			return
		}
		delay = nextBackoff(delay, d.cfg.restartDelay)
	}
}

// pause sleeps unless demesne is shutting down, in which case it reports false.
func (d *demesne) pause(s *tenant, howLong time.Duration) bool {
	t := time.NewTimer(howLong)
	defer t.Stop()
	select {
	case <-d.done:
		return false
	case <-t.C:
		return true
	}
}

func nextBackoff(d, base time.Duration) time.Duration {
	if base <= 0 {
		base = 5 * time.Second
	}
	d *= 2
	if d > 60*time.Second {
		d = 60 * time.Second
	}
	return d
}

func backoff(crashes int) time.Duration {
	d := 5 * time.Second
	for i := 1; i < crashes && d < 60*time.Second; i++ {
		d *= 2
	}
	if d > 60*time.Second {
		d = 60 * time.Second
	}
	return d
}

// stopAll takes every child down. demesne handling SIGTERM is not optional: the
// children are in their own process groups, so without this a service restart orphans
// an engine holding the card -- the exact failure this program exists to prevent.
func (d *demesne) stopAll(why string) {
	d.transition.Lock()
	defer d.transition.Unlock()
	for _, s := range d.tenants {
		if up, _ := s.isUp(); up {
			d.evictLocked(s, stopOperator, why)
		}
	}
}

// ------------------------------------------------------------------ cgroups

// prepareCgroup delegates a leaf and writes the limits. Through `sudo -n`, and a
// failure must never abort: warn and carry on unlimited, exactly as the init scripts
// did.
func (d *demesne) prepareCgroup(s *tenant) {
	m := s.specOf().cgroup
	if !m.set() {
		return
	}
	if d.cfg.cgroupPrepare == "" {
		log.Printf("warning: %s has a memory limit but no cgroup-prepare helper was found; "+
			"running unlimited", s.specOf().name)
		return
	}
	if err := makeLeaf(m); err != nil {
		// The subtree is ours, so this normally succeeds without privilege; the helper is
		// the answer when it does not (a subtree owned by root, a first start before the
		// unit has delegated anything).
		out, herr := exec.Command("sudo", "-n", d.cfg.cgroupPrepare,
			filepath.Dir(m.leaf), filepath.Base(m.leaf)).CombinedOutput()
		if herr != nil {
			s.mu.Lock()
			s.cgErr = "prepare: " + trim(string(out), 200)
			s.mu.Unlock()
			log.Printf("warning: could not prepare %s for %s: %v (direct: %v) %s (running unlimited)",
				m.leaf, s.specOf().name, herr, err, trim(string(out), 200))
			return
		}
	}
	var failed string
	for _, kv := range [][2]string{
		{"memory.high", m.high},
		{"memory.max", m.max},
		{"memory.swap.high", m.swapHigh},
	} {
		if kv[1] == "" {
			continue
		}
		if err := os.WriteFile(filepath.Join(m.leaf, kv[0]), []byte(kv[1]), 0o644); err != nil {
			failed = kv[0] + ": " + err.Error()
			log.Printf("warning: could not set %s=%s for %s: %v", kv[0], kv[1], s.specOf().name, err)
		}
	}
	s.mu.Lock()
	s.cgAt, s.cgErr = time.Now(), failed
	s.mu.Unlock()
	log.Printf("%s: memory.high=%s memory.max=%s memory.swap.high=%s in %s",
		s.specOf().name, orUnchanged(m.high), orUnchanged(m.max), orUnchanged(m.swapHigh), m.leaf)
}

// joinCgroup moves a freshly forked child into its leaf. Root is needed because a
// process cannot move itself without write access to the common ancestor's
// cgroup.procs. Called per child now (there is no supervisor daemon to inherit from).
func (d *demesne) joinCgroup(s *tenant, pid int) {
	m := s.specOf().cgroup
	if !m.set() || d.cfg.cgroupJoin == "" {
		return
	}
	// Direct first: a child of demesne is forked inside /estate/ctl, and the common
	// ancestor of that and its leaf is /estate, which this user owns. The helper (which
	// walks the tree, and needs root) is the answer for a process that came from outside.
	if err := os.WriteFile(filepath.Join(m.leaf, "cgroup.procs"), []byte(strconv.Itoa(pid)), 0o644); err != nil {
		out, herr := exec.Command("sudo", "-n", d.cfg.cgroupJoin,
			strconv.Itoa(pid), m.leaf).CombinedOutput()
		if herr != nil {
			s.mu.Lock()
			s.cgJoined, s.cgErr = false, "join: "+trim(string(out), 200)
			s.mu.Unlock()
			log.Printf("warning: could not move %s (pid %d) into %s: %v (direct: %v) %s",
				s.specOf().name, pid, m.leaf, herr, err, trim(string(out), 200))
			return
		}
	}
	s.mu.Lock()
	s.cgJoined, s.cgAt, s.cgErr = true, time.Now(), ""
	s.mu.Unlock()
}

// maintainCgroup re-asserts a fence that drifted: a child that was respawned outside its
// leaf (a supervisor restart bypasses nothing, but a person running the engine by hand
// does), or limits that something else rewrote. Cheap and idempotent -- the steady state
// writes nothing.
//
// Why it exists at all: a fence that is applied once at startup and never checked is a
// fence you believe in for exactly one boot. The estate's whole point is that the card and
// the host RAM are bounded, so "bounded" has to be a running claim.
func (d *demesne) maintainCgroup(s *tenant) {
	m := s.specOf().cgroup
	if !m.set() {
		return
	}
	if d.cfg.cgroupPrepare == "" || d.cfg.cgroupJoin == "" {
		// Nothing can be done about it. prepareCgroup has already said so once, per tenant.
		return
	}
	if _, err := os.Stat(m.leaf); err != nil {
		// The leaf is gone -- a removed subtree, a reboot's leftovers, a first start where
		// the tree was never made. Build it again rather than warning about it for ever.
		log.Printf("%s: %s is missing; preparing it again", s.specOf().name, m.leaf)
		d.prepareCgroup(s)
	}
	if _, pid := s.isUp(); pid > 0 {
		if joined, err := inCgroup(pid, m.leaf); err == nil && !joined {
			log.Printf("%s: pid %d is not in %s; moving it back", s.specOf().name, pid, m.leaf)
			d.joinCgroup(s, pid)
		}
	}
	for _, kv := range [][2]string{
		{"memory.high", m.high},
		{"memory.max", m.max},
		{"memory.swap.high", m.swapHigh},
	} {
		if kv[1] == "" {
			continue
		}
		path := filepath.Join(m.leaf, kv[0])
		got, err := os.ReadFile(path)
		if err != nil || strings.TrimSpace(string(got)) != kv[1] {
			if err := os.WriteFile(path, []byte(kv[1]), 0o644); err != nil {
				log.Printf("warning: could not restore %s=%s for %s: %v", kv[0], kv[1], s.specOf().name, err)
			}
		}
	}
}

// maintainCgroups is the timer behind maintainCgroup: a fence that is written once at
// startup and never checked is a fence you believe in for exactly one boot.
func (d *demesne) maintainCgroups() {
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-d.done:
			return
		case <-t.C:
			for _, s := range d.tenants {
				d.maintainCgroup(s)
			}
		}
	}
}

// inCgroup reports whether a pid's own membership names this leaf. It reads /proc rather
// than asking systemd or the kernel through a helper, because this must be cheap enough to
// run on a timer.
func inCgroup(pid int, leaf string) (bool, error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cgroup")
	if err != nil {
		return false, err
	}
	// /proc/<pid>/cgroup names the path RELATIVE to the cgroup root ("/estate/llama/llm"),
	// while the spec carries the absolute one. Comparing them raw never matches, which
	// would have had the maintainer re-join every child on every tick for ever.
	want := strings.TrimPrefix(leaf, cgroupMount)
	for _, line := range strings.Split(string(b), "\n") {
		if i := strings.LastIndex(line, ":"); i >= 0 && strings.TrimSpace(line[i+1:]) == want {
			return true, nil
		}
	}
	return false, nil
}

// prepareCgroupRoot makes the estate's own subtree before any leaf is set up. cgroup-prepare
// wants a leaf, so the supervisor's own leaf is the one it creates: the side effect -- and
// the point -- is that controllers are enabled on /estate, without which no child leaf can
// have memory.* files at all.
func (d *demesne) prepareCgroupRoot() {
	if d.cfg.cgroupPrepare == "" {
		return
	}
	if out, err := exec.Command("sudo", "-n", d.cfg.cgroupPrepare,
		d.cfg.cgroupRoot, "ctl").CombinedOutput(); err != nil {
		log.Printf("warning: could not prepare %s: %v %s (the estate will run unlimited)",
			d.cfg.cgroupRoot, err, trim(string(out), 200))
	}
}

func orUnchanged(s string) string {
	if s == "" {
		return "(unchanged)"
	}
	return s
}

// ------------------------------------------------------------------ helpers

func openLog(dir, name string) (*os.File, error) {
	return os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
}

func portAlive(base string) bool { return portAliveWithin(base, 750*time.Millisecond) }

func portAliveWithin(base string, timeout time.Duration) bool {
	if path := unixPath(base); path != "" {
		// Dial, never stat: a socket file left by a SIGKILL is not a live engine, and the
		// child (or the launcher) removes it. ENOENT and ECONNREFUSED are both "dead".
		c, err := net.DialTimeout("unix", path, timeout)
		if err != nil {
			return false
		}
		c.Close()
		return true
	}
	hp := uriHostPort(base)
	if hp == "" {
		return false
	}
	c, err := net.DialTimeout("tcp", hp, timeout)
	if err != nil {
		return false
	}
	c.Close()
	return true
}

// unixDialer is the transport a unix tenant's proxy needs: the request URL is a placeholder,
// and every connection goes to the socket path.
func unixDialer(path string) *http.Transport {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", path)
	}
	return tr
}

// waitPort bounds the wait for a child's port. done makes it interruptible, so a
// SIGTERM during a fifteen-minute cold load does not leave demesne hanging in spawn.
func waitPort(base string, limit time.Duration, done <-chan struct{}) error {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if portAlive(base) {
			return nil
		}
		select {
		case <-done:
			return errors.New("interrupted by shutdown")
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("timed out after %s", limit)
}

// unixPath is a tenant's AF_UNIX endpoint, or "" for a TCP one. The scheme is the only marker:
// an http URL with no port would otherwise look like a path.
// endpointName is how a tenant's address is described to an operator: a port for TCP, a socket
// path for AF_UNIX. Messages that say "port" about a socket send people to the wrong tool.
// ensureSocketDir makes the directory a unix tenant's socket lives in, and refuses one it does not
// own: /run/user/<uid> is created by pam and its children are not, so something has to own the
// directory -- and a socket directory anything else can write to is a squatting race.
func ensureSocketDir(path string) error {
	dir := filepath.Dir(path)
	fi, err := os.Stat(dir)
	if err == nil {
		if !fi.IsDir() {
			return fmt.Errorf("%s exists and is not a directory", dir)
		}
		if fi.Mode().Perm()&0o022 != 0 {
			return fmt.Errorf("%s is group- or world-writable; refusing to put a socket there", dir)
		}
		if st, ok := fi.Sys().(*syscall.Stat_t); ok && int(st.Uid) != os.Getuid() {
			return fmt.Errorf("%s belongs to uid %d, not to us", dir, st.Uid)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	return os.MkdirAll(dir, 0o700)
}

func (s *tenant) endpointHint() string {
	if path := unixPath(s.specOf().url); path != "" {
		return "ss -xlp | grep " + path
	}
	return "fuser -n tcp " + strconv.Itoa(s.port())
}

func endpointName(base string, port int) string {
	if path := unixPath(base); path != "" {
		return "socket " + path
	}
	return "port " + strconv.Itoa(port)
}

func unixPath(base string) string {
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "unix" {
		return ""
	}
	if u.Path == "" || !strings.HasPrefix(u.Path, "/") {
		return ""
	}
	return u.Path
}

func uriHostPort(base string) string {
	u, err := url.Parse(base)
	if err != nil {
		return ""
	}
	return u.Host
}

func uriPort(base string) int {
	u, err := url.Parse(base)
	if err != nil {
		return 0
	}
	if p := u.Port(); p != "" {
		n, err := strconv.Atoi(p)
		if err == nil {
			return n
		}
	}
	switch u.Scheme {
	case "https":
		return 443
	case "http":
		return 80
	}
	return 0
}

// applyTenantsFile re-reads the tenants file and adopts it: children are stopped (their replacement
// is a different process by definition), the tenant set, proxies and routing index are rebuilt, and
// the new set is live for the next request. It is deliberately a soft restart rather than a field
// diff: a tenant's command line, byte layout and cgroup are not things to mutate under a running
// child, and "no restart needed" is worth one cold start.
func (d *demesne) applyTenantsFile() bool {
	specs, notes, err := tenantsFromConfig(d.cfg)
	if err != nil {
		log.Printf("tenants file changed but is not usable (%v); keeping the running set", err)
		return false
	}
	before := len(d.tenants)
	d.transition.Lock()
	for _, t := range d.tenants {
		if up, _ := t.isUp(); up {
			d.evictLocked(t, stopOperator, "the tenants file changed")
		}
	}
	d.tenants = nil
	d.index = map[string]*tenant{}
	d.proxies = map[string]*httputil.ReverseProxy{}
	d.probes = map[string]*httputil.ReverseProxy{}
	for _, spec := range specs {
		t := &tenant{spec: spec, owner: d}
		d.tenants = append(d.tenants, t)
		d.index[spec.name] = t
		d.proxies[spec.name] = reverseProxy(d, t, spec.router)
		d.probes[spec.name] = reverseProxy(d, t, false)
	}
	d.models = newModelIndex()
	d.seedModels()
	d.transition.Unlock()
	for _, note := range notes {
		log.Printf("%s", note)
	}
	log.Printf("tenants file re-read: %d tenants (was %d); children stopped, the next request starts each one", len(d.tenants), before)
	return true
}

// watchTenantsFile notices an edit to the configured tenants file and applies it. A stat per tick is
// cheaper than any watcher, and a tenants file is edited by hand, not by a machine.
func (d *demesne) watchTenantsFile() {
	if d.cfg.tenantsFile == "" {
		return
	}
	var last time.Time
	var size int64 = -1
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-d.done:
			return
		case <-t.C:
			fi, err := os.Stat(d.cfg.tenantsFile)
			if err != nil {
				continue
			}
			if fi.ModTime().Equal(last) && fi.Size() == size {
				continue
			}
			if !last.IsZero() || size >= 0 {
				d.applyTenantsFile()
			}
			last, size = fi.ModTime(), fi.Size()
		}
	}
}
