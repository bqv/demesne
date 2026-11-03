// slot.go -- the estate: one entry per engine, and the rules about who may hold
// what.
//
// A slot is one struct with one lifecycle, because the four engines differ only in
// data:
//
//	slot     port   group  policy     routed  router  why
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
//	           weights back. A slot that holds neither (whisper) has no group and is
//	           never evicted by another slot's request. The rule is applied to the group
//	           of the slot the request actually goes to, so a model served by the small
//	           card does not evict a render from the big one.
//	a port     a slot may only spawn when its own port is free, and a port already
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
	"errors"
	"fmt"
	"io"
	"log"
	"net"
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
	policyResident               // spawn at startup, restart with backoff
)

func (p policy) String() string {
	switch p {
	case policyOnDemand:
		return "on-demand"
	case policyResident:
		return "resident"
	default:
		return "external"
	}
}

// memLimit is one slot's memory fence. A cgroup v2 leaf under a delegated subtree;
// empty fields are left alone. The zero value means "no fence".
//
// These live in the slot spec rather than in an init script because the engines are
// demesne's children now: `rc_cgroup_settings` never worked for a --user service
// anyway (openrc-run.sh:60 does not source rc-cgroup.sh under RC_USER_SERVICES), which
// is why every init script carried forty lines of start_post() to do it by hand.
type memLimit struct {
	leaf     string // /sys/fs/cgroup/demesne/<slot>
	high     string
	max      string
	swapHigh string
}

func (m memLimit) set() bool {
	return m.leaf != "" && (m.high != "" || m.max != "" || m.swapHigh != "")
}

// slotSpec is the whole of a slot's configuration. Deliberately plain data: the
// lifecycle below is generic, and a new engine is a new literal in buildSlots.
type slotSpec struct {
	name string
	url  string // base URL; also the port the child must bind

	bin  string   // empty: the slot owns no child (policyExternal)
	args []string // the child's whole command line
	env  []string // extra KEY=VALUE for the child, appended to demesne's own
	// dir is the child's working directory. A router's models.ini names its weights
	// relative to it, so this is load-bearing, not tidiness.
	dir string

	group  string // exclusivity group ("" = contends for nothing)
	vram   bool   // the slot's processes hold part of the card
	policy policy
	idle   time.Duration // evict an on-demand child after this long with no request

	routed bool // demesne may route a request to it
	// router: an engine that advertises models on /v1/models, and whose answer decides
	// which slot serves a request that names a model. sd and whisper are routed but are
	// not routers: what they serve is chosen by the path, not by a model name.
	router bool
	// fallback: proxy to it even when demesne does not own it. True for an engine
	// still supervised by OpenRC (llama), false for one demesne is meant to own and
	// simply has not been configured yet (whisper) -- which must answer 503 rather
	// than fall through to the router.
	fallback bool

	cgroup memLimit
}

// slot is the spec plus the live state. mu guards the state and is held only for
// short reads and writes; a transition (which may take minutes: a cold sd load is
// ~8 min) holds demesne.transition instead, so /status is never blocked by a load.
type slot struct {
	spec slotSpec

	mu       sync.Mutex
	child    *exec.Cmd
	up       bool
	pid      int
	started  time.Time
	readyAt  time.Time
	stopping bool // the exit was ordered, so do not count it as a crash
	foreign  bool // the port answers but the process is not ours
	lastErr  string
	restarts int
	crash    int // consecutive unexpected failures; drives the backoff
	diedAt   time.Time
	timer    *time.Timer

	inflight int64 // requests in flight (atomic): keeps the idle timer off

	// gone is closed when the child this slot last started has exited. One channel per
	// child, not one per slot: with a single slot-wide channel a token left behind by a
	// failed spawn would make the supervisor believe the NEXT child had died, and it
	// would start a duplicate beside a live engine.
	gone chan struct{}
}

// ----------------------------------------------------------------- lifecycle

func (s *slot) hostPort() string { return uriHostPort(s.spec.url) }

// port is the numeric port, for messages and duplicate-port checks.
func (s *slot) port() int { return uriPort(s.spec.url) }

func (s *slot) isUp() (bool, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.up, s.pid
}

// booting is "started, not yet answering". A resident router spends a cold load in
// this state, and a chat request that arrives then should wait rather than take a
// 502 from the proxy.
func (s *slot) booting() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.up && s.readyAt.IsZero()
}

// readySince is when this slot last became ready, or the zero time for an engine we
// never started: an external engine's readiness is someone else's event, and the only
// thing we can honestly report about it is that its port answers now.
func (s *slot) readySince() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readyAt
}

// backoffLeft implements the crash backoff, which the old sd lifecycle did not have:
// a crash was logged, marked down, and the very next request retried immediately --
// each attempt able to pay a 15-minute cold load. Now a slot that keeps dying is
// spaced out, and the message says so instead of hanging.
func (s *slot) backoffLeft() (time.Duration, int) {
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

func (s *slot) noteFailure(err error) {
	s.mu.Lock()
	s.crash++
	s.diedAt = time.Now()
	s.lastErr = err.Error()
	s.mu.Unlock()
}

func (s *slot) noteReady() {
	s.mu.Lock()
	s.crash, s.lastErr, s.foreign = 0, "", false
	s.mu.Unlock()
}

// armIdle (re)arms the idle timer. It must be called at the END of a request, not the
// start: a cold render takes ~8-10 minutes, so a five-minute timer armed before it
// would kill the child mid-load. The in-flight count is what tells the timer a
// request is still running.
func (s *slot) armIdle(d *demesne) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.armIdleLocked(d)
}

func (s *slot) armIdleLocked(d *demesne) {
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	if s.spec.idle <= 0 {
		return
	}
	s.timer = time.AfterFunc(s.spec.idle, func() {
		if atomic.LoadInt64(&s.inflight) > 0 {
			// Still rendering: start the clock again rather than killing it.
			s.armIdle(d)
			return
		}
		d.evict(s, fmt.Sprintf("no request for %s", s.spec.idle))
	})
}

// -------------------------------------------------------------- transitions
//
// demesne.transition serialises every state change on the estate: only one slot may
// cross the card at a time. It is never held across a proxy, and /status never takes
// it (it reads each slot's mu), so an eight-minute cold load cannot wedge the status
// surface that exists to explain the eight-minute cold load.

func (d *demesne) ensure(s *slot) error {
	d.transition.Lock()
	defer d.transition.Unlock()
	return d.ensureLocked(s)
}

func (d *demesne) evict(s *slot, why string) {
	d.transition.Lock()
	defer d.transition.Unlock()
	d.evictLocked(s, why)
}

func (d *demesne) ensureLocked(s *slot) error {
	if up, _ := s.isUp(); up && portAlive(s.spec.url) {
		s.armIdle(d)
		return nil
	}
	if portAlive(s.spec.url) {
		s.mu.Lock()
		s.foreign = true
		s.mu.Unlock()
		return fmt.Errorf("%s is already served by a process demesne does not own "+
			"(find it with: fuser -n tcp %d; stop it, then retry)", s.spec.url, s.port())
	}
	if !s.spec.own() {
		return fmt.Errorf("%s is not configured: give it a command line", s.spec.name)
	}
	if wait, n := s.backoffLeft(); wait > 0 {
		return fmt.Errorf("%s has failed %d time(s) in a row; retrying in %s",
			s.spec.name, n, wait.Round(time.Second))
	}
	if err := d.clearGroupLocked(s); err != nil {
		return err
	}
	return d.spawnLocked(s)
}

func (s slotSpec) own() bool { return s.bin != "" && s.policy != policyExternal }

// clearGroupLocked makes room for s: every on-demand peer in the same group is
// stopped, and if s wants the card the router is asked to unload. The order matters --
// stop the peer, wait for the VRAM to actually return, and only then start loading --
// because "the process exited" and "the driver freed the memory" are different
// moments and only the second one makes a load succeed.
func (d *demesne) clearGroupLocked(s *slot) error {
	if s.spec.group == "" {
		return nil
	}
	for _, other := range d.slots {
		if other == s || other.spec.group != s.spec.group {
			continue
		}
		if other.spec.policy != policyOnDemand {
			continue
		}
		if up, _ := other.isUp(); up {
			d.evictLocked(other, s.spec.name+" wants the "+s.spec.group)
		}
	}
	if !s.spec.vram {
		return nil
	}
	d.unloadLLM()
	if err := waitVRAMBelow(d.cfg.freeBelow, d.cfg.vramWait); err != nil {
		// Not fatal: the child may still get the card (see README on --auto-fit).
		log.Printf("warning: %v (continuing: the child may still get the card)", err)
	}
	return nil
}

func (d *demesne) spawnLocked(s *slot) error {
	if d.closing.Load() {
		return errors.New("demesne is shutting down")
	}
	if err := os.MkdirAll(d.cfg.logDir, 0o755); err != nil {
		return fmt.Errorf("log dir: %w", err)
	}
	out, err := openLog(d.cfg.logDir, s.spec.name+".log")
	if err != nil {
		return fmt.Errorf("open %s stdout: %w", s.spec.name, err)
	}
	errf, err := openLog(d.cfg.logDir, s.spec.name+".err")
	if err != nil {
		out.Close()
		return fmt.Errorf("open %s stderr: %w", s.spec.name, err)
	}

	cmd := exec.Command(s.spec.bin, s.spec.args...)
	cmd.Dir = s.spec.dir
	cmd.Stdout, cmd.Stderr = out, errf
	cmd.Env = append(os.Environ(), s.spec.env...)
	// One process group, so stopping the child also stops anything it forked. The
	// cost is that it does NOT die with demesne: an orphaned sd-server is ~12.5 GiB
	// nothing owns any more, so demesne takes every child down on the way out.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if s.spec.own() {
		log.Printf("starting %s: %s %s", s.spec.name, s.spec.bin, strings.Join(s.spec.args, " "))
	}
	if err := cmd.Start(); err != nil {
		out.Close()
		errf.Close()
		s.noteFailure(err)
		return fmt.Errorf("start %s: %w", s.spec.name, err)
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
		d.evictLocked(s, fmt.Sprintf("%s never answered on %s", s.spec.name, s.spec.url))
		s.noteFailure(err)
		return fmt.Errorf("%s never came up on %s: %w", s.spec.name, s.spec.url, err)
	}
	s.mu.Lock()
	s.readyAt = time.Now()
	s.mu.Unlock()
	s.noteReady()
	log.Printf("%s ready on %s (pid %d), vram %s", s.spec.name, s.spec.url, cmd.Process.Pid, vramString())
	s.armIdle(d)
	return nil
}

// waitReady bounds the wait for a child's port -- and watches the child while it waits.
// A plain port poll is wrong in two ways: it cannot tell "still loading" from "died on
// the way", so a startup crash would block for the whole load bound (15 minutes for sd)
// before anyone heard about it, and it cannot be interrupted, so a SIGTERM during a cold
// load would leave demesne hanging. Both are checked here.
func (d *demesne) waitReady(s *slot, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if portAlive(s.spec.url) {
			return nil
		}
		if up, _ := s.isUp(); !up {
			return errors.New("the child exited while starting")
		}
		select {
		case <-d.done:
			return errors.New("interrupted by shutdown")
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("timed out after %s", limit)
}

// reap notices an exit without waiting for a request to fail.
func (d *demesne) reap(s *slot, cmd *exec.Cmd, out, errf io.Closer, gone chan struct{}) {
	err := cmd.Wait()
	out.Close()
	errf.Close()

	s.mu.Lock()
	mine := s.child == cmd
	ordered := s.stopping
	if mine {
		s.child, s.pid, s.up = nil, 0, false
		s.readyAt = time.Time{}
		if !ordered {
			s.crash++
			s.diedAt = time.Now()
			s.lastErr = fmt.Sprintf("exited unexpectedly: %v", err)
		}
		s.stopping = false
	}
	s.mu.Unlock()
	// Always this child's own channel, whether or not the slot has moved on: the
	// waiter for this process is the only thing on the other end.
	close(gone)
	log.Printf("%s (pid %d) exited: %v", s.spec.name, cmd.Process.Pid, err)
}

// evictLocked stops the child and waits for the card to come back. SIGTERM the whole
// group, then SIGKILL if it will not go: sd-server frees its weights on exit
// (measured 12.5 GiB -> 0.03 GiB), which is the entire point of stopping it.
func (d *demesne) evictLocked(s *slot, why string) {
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
	s.stopping = true
	s.mu.Unlock()

	if why != "" {
		log.Printf("stopping %s (pid %d): %s, vram %s", s.spec.name, pid, why, vramString())
	}
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if !portAlive(s.spec.url) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if portAlive(s.spec.url) {
		log.Printf("%s (pid %d) ignored SIGTERM; killing", s.spec.name, pid)
		_ = syscall.Kill(-pid, syscall.SIGKILL)
	}

	s.mu.Lock()
	s.child, s.pid, s.up, s.readyAt, s.stopping = nil, 0, false, time.Time{}, false
	s.mu.Unlock()

	if !s.spec.vram {
		return
	}
	if err := waitVRAMBelow(d.cfg.freeBelow, d.cfg.vramWait); err != nil {
		log.Printf("warning after stopping %s: %v", s.spec.name, err)
	} else {
		log.Printf("card released, vram %s", vramString())
	}
}

// ---------------------------------------------------------------- supervisor

// supervise is the resident policy: keep the engine alive, forever, with a growing
// delay between attempts. There is exactly ONE actor per restart now, which is the
// fix for the double restart: supervise-daemon used to respawn llama every 5 s while
// recoverLLM() asked rc-service for the same restart after two proxy failures.
func (d *demesne) supervise(s *slot) {
	delay := d.cfg.restartDelay
	attempt := 0
	for {
		select {
		case <-d.done:
			return
		default:
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
			log.Printf("supervise: %s did not start: %v (retrying in %s)", s.spec.name, err, delay)
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
		// A child that lived a while was healthy; start the delay sequence again.
		if time.Since(started) > 60*time.Second {
			delay = d.cfg.restartDelay
		}
		log.Printf("supervise: %s is gone; restarting in %s", s.spec.name, delay)
		if !d.pause(s, delay) {
			return
		}
		delay = nextBackoff(delay, d.cfg.restartDelay)
	}
}

// pause sleeps unless demesne is shutting down, in which case it reports false.
func (d *demesne) pause(s *slot, howLong time.Duration) bool {
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
	for _, s := range d.slots {
		if up, _ := s.isUp(); up {
			d.evictLocked(s, why)
		}
	}
}

// ------------------------------------------------------------------ cgroups

// prepareCgroup delegates a leaf and writes the limits. Through `sudo -n`, and a
// failure must never abort: warn and carry on unlimited, exactly as the init scripts
// did.
func (d *demesne) prepareCgroup(s *slot) {
	m := s.spec.cgroup
	if !m.set() {
		return
	}
	if d.cfg.cgroupPrepare == "" {
		log.Printf("warning: %s has a memory limit but no cgroup-prepare helper was found; "+
			"running unlimited", s.spec.name)
		return
	}
	out, err := exec.Command("sudo", "-n", d.cfg.cgroupPrepare,
		filepath.Dir(m.leaf), filepath.Base(m.leaf)).CombinedOutput()
	if err != nil {
		log.Printf("warning: could not prepare %s for %s: %v %s (running unlimited)",
			m.leaf, s.spec.name, err, trim(string(out), 200))
		return
	}
	for _, kv := range [][2]string{
		{"memory.high", m.high},
		{"memory.max", m.max},
		{"memory.swap.high", m.swapHigh},
	} {
		if kv[1] == "" {
			continue
		}
		if err := os.WriteFile(filepath.Join(m.leaf, kv[0]), []byte(kv[1]), 0o644); err != nil {
			log.Printf("warning: could not set %s=%s for %s: %v", kv[0], kv[1], s.spec.name, err)
		}
	}
	log.Printf("%s: memory.high=%s memory.max=%s memory.swap.high=%s in %s",
		s.spec.name, orUnchanged(m.high), orUnchanged(m.max), orUnchanged(m.swapHigh), m.leaf)
}

// joinCgroup moves a freshly forked child into its leaf. Root is needed because a
// process cannot move itself without write access to the common ancestor's
// cgroup.procs. Called per child now (there is no supervisor daemon to inherit from).
func (d *demesne) joinCgroup(s *slot, pid int) {
	m := s.spec.cgroup
	if !m.set() || d.cfg.cgroupJoin == "" {
		return
	}
	if out, err := exec.Command("sudo", "-n", d.cfg.cgroupJoin,
		strconv.Itoa(pid), m.leaf).CombinedOutput(); err != nil {
		log.Printf("warning: could not move %s (pid %d) into %s: %v %s",
			s.spec.name, pid, m.leaf, err, trim(string(out), 200))
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
