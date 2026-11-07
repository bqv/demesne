// slot_test.go -- the lifecycle, without the engines.
//
// The children are this test binary re-exec'd: TestMain notices DEMESNE_TEST_CHILD and
// becomes a one-endpoint HTTP server that a slot can spawn, kill and restart. That is
// enough to test the parts that actually break -- spawn, readiness, idle eviction,
// supervisor restart, foreign-port refusal -- with no GPU, no models and no network.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if addr := os.Getenv("DEMESNE_TEST_CHILD"); addr != "" {
		testChild(addr)
		return
	}
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	os.Exit(m.Run())
}

// testChild is the stand-in engine: it binds the address it was given and stays up
// until something kills it, which is all a slot needs to be correct.
func testChild(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok")
	})
	log.Fatal(http.ListenAndServe(addr, mux))
}

// ------------------------------------------------------------------- fixtures

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func addr(port int) string { return fmt.Sprintf("127.0.0.1:%d", port) }

// stub turns an engine into "this test binary, told to bind port".
func stub(url string, port int) engineConfig {
	return engineConfig{url: url, bin: os.Args[0], env: []string{"DEMESNE_TEST_CHILD=" + addr(port)}}
}

func testConfig(t *testing.T) config {
	t.Helper()
	sdPort := freePort(t)
	return config{
		listen:       addr(freePort(t)),
		sd:           stub(fmt.Sprintf("http://%s", addr(sdPort)), sdPort),
		llama:        engineConfig{url: fmt.Sprintf("http://%s", addr(freePort(t)))}, // external, absent
		llamb:        engineConfig{url: fmt.Sprintf("http://%s", addr(freePort(t)))},
		whisper:      engineConfig{url: fmt.Sprintf("http://%s", addr(freePort(t)))},
		idle:         800 * time.Millisecond,
		whisperIdle:  time.Second,
		busyWait:     300 * time.Millisecond,
		vramWait:     100 * time.Millisecond,
		loadWait:     10 * time.Second,
		freeBelow:    1 << 30,
		orphanAbove:  1 << 30,
		logDir:       t.TempDir(),
		restartDelay: 50 * time.Millisecond,

		restartWindow:  time.Minute,
		restartMax:     5,
		breakerFor:     time.Second,
		failThreshold:  2,
		llmRestartWait: time.Second,
		rcService:      "/bin/false",
		llmService:     "llama",
	}
}

func waitFor(t *testing.T, limit time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", limit, what)
}

// ---------------------------------------------------------------------- tests

// A lazy slot is spawned by a request, answers, and then gives the card back by itself.
// This is sd's whole contract, and the idle timer must be armed from the END of the
// request: a cold render takes minutes, so an early timer kills the child mid-load.
func TestOnDemandSlotSpawnsAndEvicts(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)
	sd := d.slotByName("sd")

	if up, _ := sd.isUp(); up {
		t.Fatal("sd is up before any request")
	}
	if err := d.ensure(sd); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if up, pid := sd.isUp(); !up || pid <= 0 {
		t.Fatalf("sd should be up with a pid, got up=%v pid=%d", up, pid)
	}
	if !portAlive(cfg.sd.url) {
		t.Fatal("the child should be answering on its port")
	}

	// Half the idle window, still loaded.
	time.Sleep(cfg.idle / 2)
	if up, _ := sd.isUp(); !up {
		t.Fatal("evicted before the idle timeout")
	}

	// And then gone, port and all.
	waitFor(t, 5*time.Second, "idle eviction", func() bool {
		up, _ := sd.isUp()
		return !up
	})
	if portAlive(cfg.sd.url) {
		t.Fatal("the port still answers after eviction")
	}
}

// A resident slot is spawned at startup and restarted when it dies -- with ONE actor.
// The old model had supervise-daemon respawning llama every 5s while recoverLLM asked
// rc-service for the same restart; here the supervisor is the only hand on the switch.
func TestResidentSlotIsRestarted(t *testing.T) {
	cfg := testConfig(t)
	port := freePort(t)
	cfg.llama = stub(fmt.Sprintf("http://%s", addr(port)), port)

	d := newTestDemesne(t, cfg)
	llama := d.slotByName("llama")
	if llama.spec.policy != policyResident {
		t.Fatalf("llama should be resident when it has a command line, got %s", llama.spec.policy)
	}
	go d.supervise(llama)

	// Wait for READY, not merely for a bound port: a child killed before waitReady() has
	// seen its port is a STARTUP failure, which is a different code path.
	waitFor(t, 10*time.Second, "the supervised router to be ready", func() bool {
		return !llama.booting() && portAlive(cfg.llama.url)
	})
	_, first := llama.isUp()

	// Kill it the way a crash does, and watch the supervisor bring it back.
	if err := syscall.Kill(first, syscall.SIGKILL); err != nil {
		t.Fatalf("kill %d: %v", first, err)
	}
	waitFor(t, 10*time.Second, "a replacement pid", func() bool {
		up, pid := llama.isUp()
		return up && pid != first && pid > 0
	})
	waitFor(t, 10*time.Second, "the replacement to be ready", func() bool {
		return !llama.booting() && portAlive(cfg.llama.url)
	})
	llama.mu.Lock()
	restarts := llama.restarts
	llama.mu.Unlock()
	if restarts < 1 {
		t.Fatalf("the supervisor should have counted a restart, got %d", restarts)
	}
}

// A port already answered by a process demesne does not own is a refusal, not a race.
// This is the 55557 collision at runtime: two engines, one port, nothing enforcing it.
func TestForeignPortHolderIsRefused(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)
	sd := d.slotByName("sd")

	ln, err := net.Listen("tcp", uriHostPort(cfg.sd.url))
	if err != nil {
		t.Fatalf("hold the port: %v", err)
	}
	defer ln.Close()

	err = d.ensure(sd)
	if err == nil {
		t.Fatal("ensure must refuse a port it does not own")
	}
	if !strings.Contains(err.Error(), "does not own") {
		t.Fatalf("the refusal should say what is wrong, got %q", err)
	}
	if st := slotStatusOf(sd); st.Foreign == "" {
		t.Fatal("/status should report the foreign holder")
	}
}

// The durable half of the port story: two slots may not claim one port, and it is
// refused at startup rather than discovered when the second engine fails to bind.
func TestValidateRejectsAClaimedPort(t *testing.T) {
	cfg := testConfig(t)
	// validate() is consulted before anything spawns, so the command lines only need to
	// be present -- and the sd one is what validate() checks first.
	cfg.sd.args = []string{"--diffusion-model", "x"}
	cfg.whisper.url = cfg.sd.url // whisper still on 55557, the old configuration
	d := newTestDemesne(t, cfg)
	err := cfg.validate(d.slots)
	if err == nil {
		t.Fatal("two slots on one port must be refused")
	}
	if !strings.Contains(err.Error(), "port") {
		t.Fatalf("the message should explain the port, got %q", err)
	}

	// The refusing half: no sd command line at all.
	cfg2 := testConfig(t)
	cfg2.sd.args = nil
	if cfg2.validate(newTestDemesne(t, cfg2).slots) == nil {
		t.Fatal("a missing sd command line must be refused")
	}
}

// Routing is data; this pins the answers that used to be scattered through ServeHTTP --
// including whisper, which had no route at all and fell through to the llama router.
func TestResolve(t *testing.T) {
	cases := []struct {
		path string
		slot string
		kind routeKind
	}{
		{"/v1/chat/completions", "llama", kindChat},
		{"/models/load", "llama", kindChat},
		{"/v1/models", "llama", kindMetadata},
		{"/props", "llama", kindMetadata},
		{"/sdapi/v1/txt2img", "sd", kindGeneration},
		{"/sdapi/v1/img2img", "sd", kindGeneration},
		{"/v1/images/generations", "sd", kindGeneration},
		{"/sdapi/v1/progress", "sd", kindObservation},
		{"/sdapi/v1/options", "sd", kindObservation},
		{"/inference", "whisper", kindGeneration},
		{"/v1/audio/transcriptions", "whisper", kindGeneration},
	}
	for _, c := range cases {
		got := resolve(c.path)
		if got.slot != c.slot || got.kind != c.kind {
			t.Errorf("resolve(%q) = {%s %d}, want {%s %d}", c.path, got.slot, got.kind, c.slot, c.kind)
		}
	}
}

// An unconfigured whisper must say so. Before this, /inference matched no route and was
// forwarded to the llama router, which answered 404 from entirely the wrong engine.
func TestUnconfiguredWhisperAnswersPlainly(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)
	rec := httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/inference", strings.NewReader("")))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "whisper") {
		t.Fatalf("the refusal should name whisper, got %q", rec.Body.String())
	}
}

// A chat request wants the card, so it evicts the media child BEFORE it proxies.
// (There is no router in this test, so the proxy then fails -- which is fine: the
// eviction is what is being measured, and it must happen first.)
func TestChatEvictsTheMediaChild(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)
	sd := d.slotByName("sd")
	if err := d.ensure(sd); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	rec := httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{}`)))
	if up, _ := sd.isUp(); up {
		t.Fatal("sd is still holding the card after a chat request")
	}
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("want 502 from the absent router, got %d", rec.Code)
	}
}

// /status is the only status surface, so it must carry the estate and keep stadi's field
// names, and it must not need the transition lock (or a cold load would wedge it).
func TestStatusShape(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)
	if err := d.ensure(d.slotByName("sd")); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	// An external engine (llama, still on OpenRC) has to be reported from its port: the
	// engine is not in our process table, so liveness is the only honest answer.
	ln, err := net.Listen("tcp", uriHostPort(cfg.llama.url))
	if err != nil {
		t.Fatalf("stand-in router: %v", err)
	}
	defer ln.Close()

	rec := httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/status", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	var got struct {
		Version      string  `json:"version"`
		Slot         string  `json:"slot"`
		SDChildUp    bool    `json:"sd_child_up"`
		SDChildPID   int     `json:"sd_child_pid"`
		IdleTimeout  float64 `json:"idle_timeout_s"`
		MediaInFlght int64   `json:"media_in_flight"`
		Slots        []struct {
			Name   string `json:"name"`
			Up     bool   `json:"up"`
			Policy string `json:"policy"`
		} `json:"slots"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Version != version {
		t.Errorf("version = %q, want %q", got.Version, version)
	}
	if got.Slot != "sd" || !got.SDChildUp || got.SDChildPID <= 0 {
		t.Errorf("the compatible fields should describe the live child: %+v", got)
	}
	names := map[string]bool{}
	upByName := map[string]bool{}
	for _, s := range got.Slots {
		names[s.Name] = true
		upByName[s.Name] = s.Up
	}
	for _, want := range []string{"llama", "llamb", "sd", "whisper"} {
		if !names[want] {
			t.Errorf("/status should describe slot %q, got %v", want, names)
		}
	}
	if !upByName["llama"] {
		t.Error("an external engine that is answering must be reported as up")
	}
	if upByName["whisper"] {
		t.Error("whisper has no command line and nothing on its port; it must be down")
	}
}

// /release is stadi's GET, still honoured, and now takes a slot name.
func TestRelease(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)
	sd := d.slotByName("sd")
	if err := d.ensure(sd); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	rec := httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/release", nil))
	if !strings.Contains(rec.Body.String(), "sd") {
		t.Fatalf("want a release naming sd, got %q", rec.Body.String())
	}
	if up, _ := sd.isUp(); up {
		t.Fatal("sd is still up after /release")
	}

	rec = httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/slots/nope/load", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("an unknown slot should 404, got %d", rec.Code)
	}
}

// A slot that keeps dying is spaced out instead of paying a cold load per request.
func TestCrashBackoff(t *testing.T) {
	s := &slot{spec: slotSpec{name: "sd", url: "http://127.0.0.1:1"}}
	if wait, n := s.backoffLeft(); wait != 0 || n != 0 {
		t.Fatalf("a slot that never failed has no backoff, got %s/%d", wait, n)
	}
	for i := 1; i <= 6; i++ {
		if got, want := backoff(i), []time.Duration{5, 10, 20, 40, 60, 60}[i-1]*time.Second; got != want {
			t.Errorf("backoff(%d) = %s, want %s", i, got, want)
		}
	}
	s.noteFailure(fmt.Errorf("boom"))
	if wait, n := s.backoffLeft(); wait <= 0 || n != 1 {
		t.Fatalf("one crash should mean a pending wait, got %s/%d", wait, n)
	}
	s.noteReady()
	if wait, _ := s.backoffLeft(); wait != 0 {
		t.Fatalf("a ready child clears the backoff, got %s", wait)
	}
}

// stadi's default RC path resolved at startup; keep that contract honest.
func TestURIHelpers(t *testing.T) {
	if got := uriPort("http://127.0.0.1:55557"); got != 55557 {
		t.Errorf("uriPort = %d", got)
	}
	if got := uriHostPort("http://127.0.0.1:55557"); got != "127.0.0.1:55557" {
		t.Errorf("uriHostPort = %q", got)
	}
	if got := uriPort("garbage"); got != 0 {
		t.Errorf("uriPort(garbage) = %d, want 0", got)
	}
	_ = strconv.Itoa(0)
}

// Every child gets its own "gone" channel. With one channel per slot, a token left
// behind by a spawn that failed during startup would be read by the supervisor as "the
// child I just started has died" -- and it would start a second engine beside a live one.
func TestEachSpawnGetsItsOwnGoneChannel(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)
	sd := d.slotByName("sd")

	if err := d.ensure(sd); err != nil {
		t.Fatalf("first ensure: %v", err)
	}
	sd.mu.Lock()
	first := sd.gone
	sd.mu.Unlock()

	d.evict(sd, stopOperator, "the test wants a second child")
	if err := d.ensure(sd); err != nil {
		t.Fatalf("second ensure: %v", err)
	}
	sd.mu.Lock()
	second := sd.gone
	sd.mu.Unlock()

	if first == second {
		t.Fatal("two children shared one channel: the next death would be misattributed")
	}
	select {
	case <-second:
		t.Fatal("the live child's channel is already closed")
	default:
	}
	if up, _ := sd.isUp(); !up {
		t.Fatal("the second child should still be running")
	}
}

// newTestDemesne builds an estate that is torn down when the test ends, the way a
// service stop would: a stub child left running holds a port and outlives the temp
// directory it was started from.
func newTestDemesne(t *testing.T, cfg config) *demesne {
	t.Helper()
	d := newDemesne(cfg)
	t.Cleanup(func() {
		d.closing.Store(true)
		close(d.done)
		d.stopAll("the test is over")
	})
	return d
}

// The local channel is the same API on a socket: no port to collide with, and the file
// mode is the access control. This is what a sibling daemon of ours (ward) gets to use
// instead of dialling loopback TCP.
func TestUnixSocketServesTheSameAPI(t *testing.T) {
	cfg := testConfig(t)
	path := filepath.Join(t.TempDir(), "demesne.sock")
	cfg.socket = path
	d := newTestDemesne(t, cfg)

	ln, err := listenUnix(path)
	if err != nil {
		t.Fatalf("listenUnix: %v", err)
	}
	defer ln.Close()
	srv := &http.Server{Handler: d}
	go srv.Serve(ln)
	defer srv.Close()

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Fatalf("socket mode = %04o, want 0600 (the filesystem is the access control)", perm)
	}

	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", path)
		},
	}}
	resp, err := client.Get("http://demesne/status")
	if err != nil {
		t.Fatalf("GET over the socket: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200 over the socket, got %d", resp.StatusCode)
	}
	var got struct {
		Version string `json:"version"`
		Socket  string `json:"socket"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Version != version {
		t.Errorf("version over the socket = %q, want %q", got.Version, version)
	}
	if got.Socket != path {
		t.Errorf("/status socket = %q, want %q", got.Socket, path)
	}

	// A second listener must not steal the first one's socket, and a plain file in the
	// way must be reported rather than unlinked: bind(2) refuses a path that exists, and
	// the temptation is to remove whatever is there.
	if _, err := listenUnix(path); err == nil {
		t.Fatal("a live socket must not be taken over")
	}
	plain := filepath.Join(t.TempDir(), "not-a-socket")
	if err := os.WriteFile(plain, []byte("keep me"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := listenUnix(plain); err == nil {
		t.Fatal("a non-socket path must be refused")
	}
	if _, err := os.Stat(plain); err != nil {
		t.Fatal("the file in the way was deleted")
	}
}

// The card answer is a contract, not a slice of /status: four fields, and per slot one
// of three states. A field added here is a field a client comes to depend on, so the
// test pins the key set and fails when it grows -- "small and typed" has to be enforced
// by something other than good intentions.
func TestEstateCardIsTheSmallTypedAnswer(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)

	get := func(query string) map[string]json.RawMessage {
		t.Helper()
		rec := httptest.NewRecorder()
		d.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/estate/card"+query, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /estate/card%s = %d: %s", query, rec.Code, rec.Body.String())
		}
		var doc map[string]json.RawMessage
		if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return doc
	}

	doc := get("")
	for _, k := range []string{"group", "holders", "since_ms", "slots"} {
		if _, ok := doc[k]; !ok {
			t.Fatalf("missing %q in %v", k, doc)
		}
	}
	if len(doc) != 4 {
		t.Fatalf("the card grew to %d fields: %v", len(doc), doc)
	}
	var holders []string
	if err := json.Unmarshal(doc["holders"], &holders); err != nil {
		t.Fatalf("holders is not an array: %s", doc["holders"])
	}
	if len(holders) != 0 {
		t.Fatalf("an idle group has holders %v", holders)
	}
	var since int64
	if err := json.Unmarshal(doc["since_ms"], &since); err != nil || since != 0 {
		t.Fatalf("idle since_ms = %s, want 0", doc["since_ms"])
	}

	if err := d.ensure(d.slotByName("sd")); err != nil {
		t.Fatalf("ensure(sd): %v", err)
	}
	before := time.Now().Add(-time.Minute).UnixMilli()
	doc = get("")
	if err := json.Unmarshal(doc["holders"], &holders); err != nil {
		t.Fatalf("holders: %v", err)
	}
	if len(holders) != 1 || holders[0] != "sd" {
		t.Fatalf("holders = %v, want [sd]", holders)
	}
	if err := json.Unmarshal(doc["since_ms"], &since); err != nil {
		t.Fatalf("since_ms: %v", err)
	}
	if since < before || since > time.Now().UnixMilli() {
		t.Fatalf("since_ms = %d, want the moment sd became ready", since)
	}
	if raw, _ := json.Marshal(doc); strings.Contains(string(raw), "whisper") {
		t.Fatal("whisper contends for no card, so it must not appear in a card answer")
	}

	aux := get("?group=aux")
	if raw, _ := json.Marshal(aux); !strings.Contains(string(raw), `"llamb"`) {
		t.Fatalf("the aux group should name llamb: %s", raw)
	}
}

// modelRouter is a stand-in engine that advertises one model and names itself when asked
// to chat, so a test can tell which slot a request actually reached.
func modelRouter(name, id string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"object":"list","data":[{"id":"`+id+`","object":"model"}]}`)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		io.WriteString(w, name+" "+string(body))
	})
	return mux
}

// The number in front is one port now, so the model NAME has to choose the engine -- and a
// model on the small card must not disturb the big one's card at all.
func TestTheModelNameDecidesTheEngine(t *testing.T) {
	big := httptest.NewServer(modelRouter("llama", "big-model"))
	defer big.Close()
	small := httptest.NewServer(modelRouter("llamb", "small-model"))
	defer small.Close()

	cfg := testConfig(t)
	cfg.llama = engineConfig{url: big.URL}
	cfg.llamb = engineConfig{url: small.URL}
	d := newTestDemesne(t, cfg)
	d.models = newModelIndex()
	d.refreshModels()

	if got := d.slotForModel("small-model"); got != "llamb" {
		t.Fatalf("small-model routes to %q, want llamb", got)
	}
	if got := d.slotForModel("big-model"); got != "llama" {
		t.Fatalf("big-model routes to %q, want llama", got)
	}

	if err := d.ensure(d.slotByName("sd")); err != nil {
		t.Fatalf("ensure(sd): %v", err)
	}
	rec := httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"small-model","messages":[]}`)))
	body := rec.Body.String()
	if !strings.HasPrefix(body, "llamb ") {
		t.Fatalf("the aux model reached %q, want the llamb stub", body)
	}
	if !strings.Contains(body, `"model":"small-model"`) {
		t.Fatalf("the request body did not survive the model peek: %q", body)
	}
	if up, _ := d.slotByName("sd").isUp(); !up {
		t.Fatal("an aux-card request evicted the media child on the other card")
	}

	rec = httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"big-model"}`)))
	if up, _ := d.slotByName("sd").isUp(); up {
		t.Fatal("a big-card request left the media child holding the card")
	}

	rec = httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"nobody-has-this"}`)))
	if got := rec.Body.String(); !strings.HasPrefix(got, "llama ") {
		t.Fatalf("an unknown model went to %q, want the default router", got)
	}
}

// One port, one list: /v1/models is the union of the routers' answers, because a client
// that dials the one number should see every model it can ask for.
func TestModelsListIsTheWholeEstate(t *testing.T) {
	big := httptest.NewServer(modelRouter("llama", "big-model"))
	defer big.Close()
	small := httptest.NewServer(modelRouter("llamb", "small-model"))
	defer small.Close()

	cfg := testConfig(t)
	cfg.llama = engineConfig{url: big.URL}
	cfg.llamb = engineConfig{url: small.URL}
	d := newTestDemesne(t, cfg)

	rec := httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/models = %d: %s", rec.Code, rec.Body.String())
	}
	for _, want := range []string{"big-model", "small-model"} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("%q missing from %s", want, rec.Body.String())
		}
	}
}

// writeConf writes a conf.d in the shape OpenRC reads: plain shell assignments.
func writeConf(t *testing.T, path string, kv map[string]string) {
	t.Helper()
	var b strings.Builder
	for k, v := range kv {
		fmt.Fprintf(&b, "export %s=%q\n", k, v)
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatalf("write conf: %v", err)
	}
}

// sdConf is the running test estate's own sd slot, expressed as a conf file: the reload
// should find nothing to do with it.
func sdConf(cfg config) map[string]string {
	return map[string]string{
		"DEMESNE_SD":     cfg.sd.url,
		"DEMESNE_SD_BIN": cfg.sd.bin,
		"DEMESNE_SD_ENV": strings.Join(cfg.sd.env, " "),
		"DEMESNE_IDLE":   cfg.idle.String(),
	}
}

func TestReloadLeavesAMatchingChildAlone(t *testing.T) {
	cfg := testConfig(t)
	path := filepath.Join(t.TempDir(), "demesne.conf")
	cfg.confFile = path
	d := newTestDemesne(t, cfg)
	if err := d.ensure(d.slotByName("sd")); err != nil {
		t.Fatalf("ensure(sd): %v", err)
	}

	kv := sdConf(cfg)
	kv["DEMESNE_IDLE"] = "7s" // a clock, not a command line: applies without a restart
	writeConf(t, path, kv)

	rep, err := d.reload()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if contains(rep.Restarted, "sd") {
		t.Fatalf("reload restarted sd for a clock change: %+v", rep)
	}
	if up, _ := d.slotByName("sd").isUp(); !up {
		t.Fatal("reload stopped a child it had no reason to touch")
	}
	if got := d.slotByName("sd").specOf().idle; got != 7*time.Second {
		t.Fatalf("sd idle = %v, want 7s", got)
	}
}

func TestReloadRestartsAChildWhoseArgsMoved(t *testing.T) {
	cfg := testConfig(t)
	path := filepath.Join(t.TempDir(), "demesne.conf")
	cfg.confFile = path
	d := newTestDemesne(t, cfg)
	if err := d.ensure(d.slotByName("sd")); err != nil {
		t.Fatalf("ensure(sd): %v", err)
	}

	kv := sdConf(cfg)
	kv["DEMESNE_SD_ARGS"] = "--a-different-command-line"
	writeConf(t, path, kv)

	rep, err := d.reload()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !contains(rep.Restarted, "sd") {
		t.Fatalf("a new command line did not restart sd: %+v", rep)
	}
	if up, _ := d.slotByName("sd").isUp(); up {
		t.Fatal("the old child is still running with the old argv")
	}
	if got := d.slotByName("sd").specOf().args; len(got) != 1 || got[0] != "--a-different-command-line" {
		t.Fatalf("sd args = %v", got)
	}
}

// The way in cannot move under a running instance: every client would lose its connection
// mid-stream. It is refused, and the rest of the file still applies.
func TestReloadRefusesTheWayIn(t *testing.T) {
	cfg := testConfig(t)
	path := filepath.Join(t.TempDir(), "demesne.conf")
	cfg.confFile = path
	d := newTestDemesne(t, cfg)
	before := d.cfg.listen

	kv := sdConf(cfg)
	kv["DEMESNE_LISTEN"] = "127.0.0.1:1"
	writeConf(t, path, kv)

	rep, err := d.reload()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !contains(rep.Refused, "listen") && !strings.HasPrefix(strings.Join(rep.Refused, " "), "listen") {
		t.Fatalf("a moved listen address was not refused: %+v", rep)
	}
	if d.cfg.listen != before {
		t.Fatalf("listen moved from %s to %s", before, d.cfg.listen)
	}
}

// A running daemon cannot see an edited conf.d through os.Getenv, so it reads the file with
// a shell -- and the file is the whole truth: a variable it drops must not survive from the
// environment the daemon was started with.
func TestReadConfFileIsTheWholeTruth(t *testing.T) {
	path := filepath.Join(t.TempDir(), "demesne.conf")
	if err := os.WriteFile(path, []byte("export DEMESNE_SD_BIN=\"/bin/true\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	values, err := readConfFile(path)
	if err != nil {
		t.Fatalf("readConfFile: %v", err)
	}
	if values["DEMESNE_SD_BIN"] != "/bin/true" {
		t.Fatalf("values = %v", values)
	}
	if _, ok := values["DEMESNE_SD_ARGS"]; ok {
		t.Fatal("a variable the file does not set appeared in the values")
	}
}

func contains(hay []string, needle string) bool {
	for _, s := range hay {
		if s == needle {
			return true
		}
	}
	return false
}

// A router that is up with nothing loaded holds a port and about 28 MiB, not the card. The
// distinction is the whole point of the field: a client told "llama holds it" would refuse
// to render on a free card.
func TestAnIdleRouterDoesNotHoldTheCard(t *testing.T) {
	withStatus := func(id, status string) http.Handler {
		mux := http.NewServeMux()
		mux.HandleFunc("/v1/models", func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"object":"list","data":[{"id":"`+id+`","status":{"value":"`+status+`"}}]}`)
		})
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") })
		return mux
	}

	idle := httptest.NewServer(withStatus("big-model", "unloaded"))
	defer idle.Close()
	busy := httptest.NewServer(withStatus("small-model", "loaded"))
	defer busy.Close()

	cfg := testConfig(t)
	cfg.llama = engineConfig{url: idle.URL}
	cfg.llamb = engineConfig{url: busy.URL}
	d := newTestDemesne(t, cfg)
	d.models = newModelIndex()
	d.refreshModels()

	card := func(group string) estateCard {
		t.Helper()
		rec := httptest.NewRecorder()
		d.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/estate/card?group="+group, nil))
		var out estateCard
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return out
	}

	if got := card("gpu"); len(got.Holders) != 0 {
		t.Fatalf("an idle router holds the card: %v (state is still %q)", got.Holders, got.Slots[0].State)
	}
	if got := card("aux"); len(got.Holders) != 1 || got.Holders[0] != "llamb" {
		t.Fatalf("a router with a resident model does not hold its card: %v", got.Holders)
	}
}

// A probe is not a claimant. The catch-all used to be chat, so GET /slots or /metrics on the
// front door evicted a running render before answering -- a monitoring loop could kill a
// render simply by looking at the estate.
func TestAProbeDoesNotTakeTheCard(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)
	sd := d.slotByName("sd")
	if err := d.ensure(sd); err != nil {
		t.Fatalf("ensure(sd): %v", err)
	}

	for _, path := range []string{"/slots", "/metrics", "/tokenize", "/detokenize", "/apply-template"} {
		if got := resolve(path); got.kind != kindProxy {
			t.Errorf("resolve(%q) = kind %d, want kindProxy", path, got.kind)
		}
		// llama is absent in this fixture, so the proxy answers 502 -- what matters is
		// that the media child is still holding the card afterwards.
		rec := httptest.NewRecorder()
		d.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if up, _ := sd.isUp(); !up {
			t.Fatalf("GET %s evicted the media child", path)
		}
	}

	// And the thing that IS a claim still claims.
	if got := resolve("/v1/chat/completions"); got.kind != kindChat {
		t.Fatalf("resolve(chat) = %d, want kindChat", got.kind)
	}
	rec := httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{}`)))
	if up, _ := sd.isUp(); up {
		t.Fatal("a chat request left the media child holding the card")
	}
}

// last_transition says WHY a slot is not running, in a closed vocabulary, and says nothing
// while it is running. "chat-claims-card" is the one a reader most wants explained: an idle
// eviction and a divested render look identical in `last_error`, which is empty for both.
func TestLastTransitionSaysWhyTheChildStopped(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)
	sd := d.slotByName("sd")

	transition := func() string {
		t.Helper()
		rec := httptest.NewRecorder()
		d.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/slots/sd", nil))
		var st slotStatus
		if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return string(st.LastTransition)
	}

	if err := d.ensure(sd); err != nil {
		t.Fatalf("ensure(sd): %v", err)
	}
	if got := transition(); got != "" {
		t.Fatalf("a running slot reports last_transition %q, want empty", got)
	}

	rec := httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/slots/sd/stop", nil))
	if got := transition(); got != string(stopOperator) {
		t.Fatalf("after an operator stop: %q, want %q", got, stopOperator)
	}

	if err := d.ensure(sd); err != nil {
		t.Fatalf("ensure(sd) again: %v", err)
	}
	if got := transition(); got != "" {
		t.Fatalf("readiness did not clear last_transition: %q", got)
	}

	// A chat request is the interesting one: it evicts the media child without it failing.
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{}`)))
	if got := transition(); got != string(stopChat) {
		t.Fatalf("after a chat claimed the card: %q, want %q", got, stopChat)
	}
}

// A slot that is down is down for one of four reasons, and "never asked since boot" is the
// same situation the idle timer produces -- so the field is total for a down slot rather than
// leaving a client to guess at an absent one.
func TestADownSlotAlwaysSaysWhy(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)

	transition := func() string {
		t.Helper()
		rec := httptest.NewRecorder()
		d.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/slots/sd", nil))
		var st slotStatus
		if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return string(st.LastTransition)
	}

	if got := transition(); got != string(stopIdle) {
		t.Fatalf("a slot that has never run reports %q, want %q", got, stopIdle)
	}

	// A failure, then an operator stop of the already-down slot: the failure is the last real
	// event and must not be papered over.
	sd := d.slotByName("sd")
	sd.mu.Lock()
	sd.stopped, sd.lastErr = stopFailed, "exited unexpectedly: boom"
	sd.mu.Unlock()
	d.evict(sd, stopOperator, "operator stop")
	if got := transition(); got != string(stopFailed) {
		t.Fatalf("an operator stop masked a failure: %q", got)
	}
}

// fimServer is a stand-in for llama.cpp's /infill: it records the request it was given and
// answers in the two shapes a build may use -- SSE by default, or a JSON body when asked with
// a (non-standard for /infill) stream:false.
type fimServer struct {
	*httptest.Server
	mu       sync.Mutex
	lastBody map[string]any
	jsonMode bool
}

func newFimServer(t *testing.T, text string, jsonMode bool) *fimServer {
	t.Helper()
	f := &fimServer{jsonMode: jsonMode}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		f.lastBody = body
		f.mu.Unlock()
		if r.URL.Path != "/infill" {
			http.NotFound(w, r)
			return
		}
		if f.jsonMode {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"content": text})
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		for _, piece := range strings.SplitAfter(text, " ") {
			if piece == "" {
				continue
			}
			b, _ := json.Marshal(map[string]any{"content": piece})
			fmt.Fprintf(w, "data: %s\n\n", b)
			if flusher != nil {
				flusher.Flush()
			}
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	return f
}

func (f *fimServer) last() map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastBody
}

// Mistral's FIM dialect is reachable at all: prompt+suffix become input_prefix/input_suffix,
// the router gets its model field, and the text comes back enclosed in a chat.completion.
func TestFIMIsATranslationNotAProxy(t *testing.T) {
	up := newFimServer(t, "a + b", false)
	defer up.Close()

	cfg := testConfig(t)
	cfg.llama = engineConfig{url: up.URL}
	d := newTestDemesne(t, cfg)
	d.models = newModelIndex()
	d.refreshModels()

	rec := httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/fim/completions", strings.NewReader(
		`{"model":"big-model","prompt":"def add(a, b):\n    return ","suffix":"\n","max_tokens":16,"temperature":0.2}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	sent := up.last()
	if sent["input_prefix"] != "def add(a, b):\n    return " || sent["input_suffix"] != "\n" {
		t.Fatalf("the prefix/suffix were not translated: %v", sent)
	}
	if sent["model"] != "big-model" {
		t.Fatalf("the router did not get its model field: %v", sent)
	}
	if sent["n_predict"] != float64(16) || sent["stream"] != true {
		t.Fatalf("token budget or streaming flag wrong: %v", sent)
	}

	var out struct {
		Object  string `json:"object"`
		Model   string `json:"model"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
				Role    string `json:"role"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Object != "chat.completion" || out.Model != "big-model" {
		t.Fatalf("envelope wrong: %+v", out)
	}
	if len(out.Choices) != 1 || out.Choices[0].Message.Content != "a + b" || out.Choices[0].Message.Role != "assistant" {
		t.Fatalf("choices wrong: %+v", out.Choices)
	}
	if out.Choices[0].FinishReason != "stop" {
		t.Fatalf("finish_reason = %q", out.Choices[0].FinishReason)
	}
}

// Streaming uses Mistral's own dialect for a streaming FIM request: data-only events,
// each an OpenAI-shaped chunk, terminated by [DONE].
func TestFIMStreamsOpenAIChunks(t *testing.T) {
	up := newFimServer(t, "one two", false)
	defer up.Close()

	cfg := testConfig(t)
	cfg.llama = engineConfig{url: up.URL}
	d := newTestDemesne(t, cfg)

	rec := httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/fim/completions", strings.NewReader(
		`{"model":"big-model","prompt":"x","suffix":"y","stream":true}`)))
	body := rec.Body.String()
	if !strings.Contains(body, `"object":"chat.completion.chunk"`) {
		t.Fatalf("no chunks in %q", body)
	}
	if !strings.HasSuffix(body, "data: [DONE]\n\n") {
		t.Fatalf("stream does not end with [DONE]: %q", body)
	}
	if !strings.Contains(body, `"content":"one"`) && !strings.Contains(body, `"content":"one "`) {
		t.Fatalf("the text never arrived: %q", body)
	}
}

// A JSON body from a build that honours stream:false is read too, so the adapter does not
// depend on which llama.cpp build is behind it.
func TestFIMReadsAJSONUpstream(t *testing.T) {
	up := newFimServer(t, "  return a + b", true)
	defer up.Close()

	cfg := testConfig(t)
	cfg.llama = engineConfig{url: up.URL}
	d := newTestDemesne(t, cfg)

	rec := httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/fim/completions", strings.NewReader(
		`{"model":"big-model","prompt":"def f():","suffix":"\n"}`)))
	if !strings.Contains(rec.Body.String(), "return a + b") {
		t.Fatalf("json upstream not read: %s", rec.Body.String())
	}
}

// The engine's own refusal is the true answer and is passed through with its status: a model
// without FIM tokens says 501, and pretending otherwise would be a worse lie than an error.
func TestFIMPassesThroughTheEnginesRefusal(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotImplemented)
		_, _ = io.WriteString(w, `{"error":{"code":501,"message":"Infill is not supported by this model: middle token is missing. "}}`)
	}))
	defer up.Close()

	cfg := testConfig(t)
	cfg.llama = engineConfig{url: up.URL}
	d := newTestDemesne(t, cfg)

	rec := httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/fim/completions", strings.NewReader(
		`{"model":"m","prompt":"a","suffix":"b"}`)))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status %d, want 501", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "middle token is missing") {
		t.Fatalf("the engine's reason was lost: %s", rec.Body.String())
	}
}

// And FIM claims the card like any other generation: it must take it from a running render.
func TestFIMClaimsTheCard(t *testing.T) {
	up := newFimServer(t, "x", false)
	defer up.Close()

	cfg := testConfig(t)
	cfg.llama = engineConfig{url: up.URL}
	d := newTestDemesne(t, cfg)
	sd := d.slotByName("sd")
	if err := d.ensure(sd); err != nil {
		t.Fatalf("ensure(sd): %v", err)
	}
	rec := httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/fim/completions", strings.NewReader(
		`{"model":"big-model","prompt":"a","suffix":"b"}`)))
	if up, _ := sd.isUp(); up {
		t.Fatal("a FIM request left the media child holding the card")
	}
}

// A caller that gives up says nothing about the engine. On 2026-11-07 two client timeouts,
// five minutes apart, tripped the breaker and the watch killed a router 14.65 GiB into a cold
// load; every retry after that had no model and no chance. The 27B rung was never at fault.
func TestAClientCancelIsNotAnEngineFailure(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)
	p := d.proxies["llama"]

	// The client went away: the proxy's error handler must leave the watch alone.
	for i := 0; i < cfg.failThreshold+3; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		ctx, cancel := context.WithCancel(req.Context())
		cancel()
		req = req.WithContext(ctx)
		rec := httptest.NewRecorder()
		p.ErrorHandler(rec, req, context.Canceled)
	}
	if open, wait := d.llm.failing(); open {
		t.Fatalf("client cancellations opened the breaker (retry after %v)", wait)
	}

	// A genuine transport failure still counts, or the breaker would protect nothing.
	for i := 0; i < cfg.failThreshold; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		rec := httptest.NewRecorder()
		p.ErrorHandler(rec, req, errors.New("dial tcp 127.0.0.1:55556: connect: connection refused"))
	}
	if open, _ := d.llm.failing(); !open {
		t.Fatal("real transport failures no longer trip the breaker")
	}
}

// slowEngine answers like llama.cpp's router during a cold load: nothing, for a while, and
// then a stream. It records whether ITS context was cancelled before it answered, which is how
// the second test proves the load survived the caller.
type slowEngine struct {
	*httptest.Server
	delay                 time.Duration
	mu                    sync.Mutex
	cancelledWhenAnswered bool
	answered              bool
	path                  string
}

func newSlowEngine(delay time.Duration, text string) *slowEngine {
	e := &slowEngine{delay: delay}
	e.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(e.delay)
		e.mu.Lock()
		e.path = r.URL.Path
		e.cancelledWhenAnswered = r.Context().Err() != nil
		e.answered = true
		e.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", text)
		if flusher != nil {
			flusher.Flush()
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	return e
}

func (e *slowEngine) record() (bool, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.answered, e.cancelledWhenAnswered
}

// The client must be answered BEFORE the engine is. A caller with a 300 s header deadline is
// the thing that killed session d9ff4d92: llama.cpp says nothing while it loads a 27B rung.
func TestAStreamingChatAnswersBeforeTheEngineDoes(t *testing.T) {
	up := newSlowEngine(600*time.Millisecond, "hello")
	defer up.Close()

	cfg := testConfig(t)
	cfg.llama = engineConfig{url: up.URL}
	d := newTestDemesne(t, cfg)
	srv := httptest.NewServer(d)
	defer srv.Close()

	started := time.Now()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"m","stream":true,"messages":[]}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("the front door did not answer: %v", err)
	}
	defer resp.Body.Close()
	waited := time.Since(started)
	if waited > 400*time.Millisecond {
		t.Fatalf("headers took %v; the engine had not even answered yet (delay 600ms)", waited)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type %q", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "hello") {
		t.Fatalf("the stream never carried the engine's text: %q", string(body))
	}
	if !strings.Contains(string(body), "data: [DONE]") {
		t.Fatalf("no [DONE] in %q", string(body))
	}
}

// And the load is shared work: a caller that gives up must not destroy it. This is the half
// that would have saved the 14.65 GiB load the watch killed.
func TestACancelDoesNotAbortTheLoad(t *testing.T) {
	up := newSlowEngine(500*time.Millisecond, "late")
	defer up.Close()

	cfg := testConfig(t)
	cfg.llama = engineConfig{url: up.URL}
	d := newTestDemesne(t, cfg)
	srv := httptest.NewServer(d)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"m","stream":true,"messages":[]}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		// We asked to give up after 100 ms and the engine answers at 500 ms; the read must
		// fail, which is the caller giving up.
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
	}

	// Wait for the engine to have answered, then look at what it saw.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if answered, _ := up.record(); answered {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	answered, cancelled := up.record()
	if !answered {
		t.Fatal("the engine never answered at all")
	}
	if cancelled {
		t.Fatal("the engine's request was cancelled when the client gave up: the load died with it")
	}
}

// callRPC posts one JSON-RPC request at the handler and decodes the envelope.
func callRPC(t *testing.T, d *demesne, body string) (int, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(body)))
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

func TestRPCAnswersTheTypedMethods(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)

	code, out := callRPC(t, d, `{"jsonrpc":"2.0","id":1,"method":"estate.card","params":{"group":"gpu"}}`)
	if code != http.StatusOK || out["error"] != nil {
		t.Fatalf("estate.card: %d %v", code, out)
	}
	card, ok := out["result"].(map[string]any)
	if !ok || card["group"] != "gpu" {
		t.Fatalf("estate.card result: %v", out["result"])
	}

	code, out = callRPC(t, d, `{"jsonrpc":"2.0","id":2,"method":"estate.status"}`)
	if code != http.StatusOK || out["error"] != nil {
		t.Fatalf("estate.status: %d %v", code, out)
	}
	status := out["result"].(map[string]any)
	if status["version"] != version {
		t.Fatalf("estate.status version = %v", status["version"])
	}
	if slots, ok := status["slots"].([]any); !ok || len(slots) != 4 {
		t.Fatalf("estate.status slots = %v", status["slots"])
	}

	// The method that stops an engine must reach the same code path as the HTTP verb, so the
	// slot's own account of itself agrees.
	if err := d.ensure(d.slotByName("sd")); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	code, out = callRPC(t, d, `{"jsonrpc":"2.0","id":3,"method":"slots.stop","params":{"slot":"sd"}}`)
	if code != http.StatusOK || out["error"] != nil {
		t.Fatalf("slots.stop: %d %v", code, out)
	}
	if up, _ := d.slotByName("sd").isUp(); up {
		t.Fatal("slots.stop left the child running")
	}
	if got := d.slotByName("sd").specOf().name; got != "sd" {
		t.Fatalf("wrong slot: %s", got)
	}

	_, out = callRPC(t, d, `{"jsonrpc":"2.0","id":4,"method":"slots.status","params":{"slot":"sd"}}`)
	st := out["result"].(map[string]any)
	if st["last_transition"] != string(stopOperator) {
		t.Fatalf("last_transition after an RPC stop = %v", st["last_transition"])
	}
}

func TestRPCErrorsAreJSONRPC(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)

	cases := []struct {
		name string
		body string
		code float64
	}{
		{"no method", `{"jsonrpc":"2.0","id":1,"method":"nope"}`, rpcNoMethod},
		{"unknown slot", `{"jsonrpc":"2.0","id":2,"method":"slots.status","params":{"slot":"zzz"}}`, rpcBadParams},
		{"missing slot", `{"jsonrpc":"2.0","id":3,"method":"slots.load","params":{}}`, rpcBadParams},
	}
	for _, c := range cases {
		_, out := callRPC(t, d, c.body)
		e, ok := out["error"].(map[string]any)
		if !ok {
			t.Fatalf("%s: no error in %v", c.name, out)
		}
		if e["code"] != c.code {
			t.Fatalf("%s: code %v, want %v", c.name, e["code"], c.code)
		}
	}
}

// A batch is answered in order, and a notification is run but not answered.
func TestRPCBatchAndNotification(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)

	rec := httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(
		`[{"jsonrpc":"2.0","id":1,"method":"estate.card"},{"jsonrpc":"2.0","id":2,"method":"estate.status"}]`)))
	var batch []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &batch); err != nil {
		t.Fatalf("batch decode: %v (%s)", err, rec.Body.String())
	}
	if len(batch) != 2 || batch[0]["id"] != float64(1) || batch[1]["id"] != float64(2) {
		t.Fatalf("batch out of order: %v", batch)
	}

	rec = httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(
		`{"jsonrpc":"2.0","method":"estate.reload"}`)))
	if rec.Code != http.StatusNoContent || rec.Body.Len() != 0 {
		t.Fatalf("a notification was answered: %d %q", rec.Code, rec.Body.String())
	}
}

// A port answered by somebody else is a refusal, not a race. Spawning anyway gets a child that
// cannot bind, dies with a bare "exit status 1", and takes the supervisor into a crash loop
// that looks like the engine's fault -- which is exactly what a 1d19h orphan caused on
// 2026-11-07, with nothing in any log to say so.
func TestAForeignPortRefusesTheSpawn(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)
	s := d.slotByName("sd")

	holder, err := net.Listen("tcp", s.hostPort())
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer holder.Close()

	err = d.ensure(s)
	if err == nil {
		t.Fatal("ensure() spawned a child onto a port somebody else holds")
	}
	if !strings.Contains(err.Error(), "demesne does not own") {
		t.Fatalf("the refusal should say what is wrong, got %q", err)
	}
	if up, _ := s.isUp(); up {
		t.Fatal("a child was started anyway")
	}
	if st := slotStatusOf(s); st.Foreign == "" {
		t.Fatal("/status does not report the foreign holder")
	}
}

// Readiness must be OUR child answering. A port that answers while the child is already dead
// belongs to somebody else, and mistaking it for a successful start is how an orphan gets
// adopted as an engine.
func TestReadinessNeedsOurOwnChild(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)
	s := d.slotByName("sd")

	holder, err := net.Listen("tcp", s.hostPort())
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer holder.Close()

	err = d.waitReady(s, 600*time.Millisecond)
	if err == nil {
		t.Fatal("waitReady() accepted a port answered by a stranger")
	}
	if !strings.Contains(err.Error(), "child exited") {
		t.Fatalf("want the child exit as the reason, got %q", err)
	}
}

// The port's holder must be findable the way ss finds it, or nothing else in reclaim.go means
// anything.
func TestProcPortPIDFindsTheHolder(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port

	pid, ok := procPortPID(port)
	if !ok {
		t.Fatalf("no holder found for port %d", port)
	}
	if pid != os.Getpid() {
		t.Fatalf("holder pid = %d, want this process (%d)", pid, os.Getpid())
	}
}

// Killing is allowed only on evidence. A listener that is not in the slot's own cgroup is a
// stranger, and a refusal is the correct answer -- even though it leaves the slot stuck.
func TestReclaimRefusesAStranger(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)
	s := d.slotByName("sd")

	ln, err := net.Listen("tcp", s.hostPort())
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	killed, err := d.reclaimSlot(s)
	if killed {
		t.Fatal("reclaim killed a process it could not prove was ours")
	}
	if err == nil {
		t.Fatal("reclaim refused silently")
	}
	if !strings.Contains(err.Error(), "refusing") {
		t.Fatalf("the refusal should say it refused: %q", err)
	}
	// The stranger is this test process, so "still alive" is the assertion that matters.
	if _, ok := procPortPID(s.port()); !ok {
		t.Fatal("the listener was killed anyway")
	}
}

// And the spawn refusal must now carry the reason from the reclaim attempt.
func TestSpawnRefusalNamesTheReason(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)
	s := d.slotByName("sd")

	ln, err := net.Listen("tcp", s.hostPort())
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	err = d.ensure(s)
	if err == nil {
		t.Fatal("ensure() succeeded over a held port")
	}
	if !strings.Contains(err.Error(), "port_held_elsewhere") {
		t.Fatalf("the refusal should point at /status: %q", err)
	}
}

func findLoad(t *testing.T, loads []modelLoad, slot, model string) *modelLoad {
	t.Helper()
	for i := range loads {
		if loads[i].Slot == slot && loads[i].Model == model {
			return &loads[i]
		}
	}
	return nil
}

// The Android card needs one entry per model load, not just a slot that happens to be busy.
func TestLoadsFollowAModelFromLoadingToReady(t *testing.T) {
	b := newLoadBoard("")
	b.observe("llama", map[string]string{"M1": "loading"})
	got := b.snapshot()
	e := findLoad(t, got, "llama", "M1")
	if e == nil || e.Phase != loadLoading {
		t.Fatalf("no loading entry: %+v", got)
	}
	if e.StartedMS == 0 || e.FinishedMS != nil {
		t.Fatalf("started/finished wrong: %+v", e)
	}
	b.observe("llama", map[string]string{"M1": "loaded"})
	e = findLoad(t, b.snapshot(), "llama", "M1")
	if e.Phase != loadReady || e.FinishedMS == nil {
		t.Fatalf("not ready after load: %+v", e)
	}
}

// A load that dies must say so rather than vanish, or the card freezes on "loading".
func TestLoadsReportAFailedLoad(t *testing.T) {
	b := newLoadBoard("")
	b.observe("llama", map[string]string{"M1": "loading"})
	b.observe("llama", map[string]string{"M1": "unloaded"})
	e := findLoad(t, b.snapshot(), "llama", "M1")
	if e.Phase != loadFailed || e.Error == nil {
		t.Fatalf("unloaded mid-load is not a failure: %+v", e)
	}
}

// A request for a model the router does not hold yet is queued BEFORE any load starts; this is
// the only phase the router itself cannot report.
func TestALoadIsQueuedWhenARequestNamesIt(t *testing.T) {
	b := newLoadBoard("")
	b.observe("llama", map[string]string{"M1": "loaded"})
	b.noteRequest("llama", "M1")
	if findLoad(t, b.snapshot(), "llama", "M1") != nil {
		t.Fatal("a request for an already-loaded model invented a load")
	}
	b.noteRequest("llama", "M2")
	e := findLoad(t, b.snapshot(), "llama", "M2")
	if e == nil || e.Phase != loadQueued {
		t.Fatalf("no queued entry for M2: %+v", b.snapshot())
	}
}

// The child log is the only source for the line and the progress, and for why a load failed.
func TestLoadsCarryTheChildLog(t *testing.T) {
	dir := t.TempDir()
	log := "srv  load:   --model\n" +
		"load_tensors: loading model tensors, this can take a while...\n" +
		"llama_model_loader: loaded meta data (45%)\n"
	if err := os.WriteFile(filepath.Join(dir, "llama.err"), []byte(log), 0o600); err != nil {
		t.Fatal(err)
	}
	b := newLoadBoard(dir)
	b.observe("llama", map[string]string{"M1": "loading"})
	b.refreshLogs([]string{"llama"})
	e := findLoad(t, b.snapshot(), "llama", "M1")
	if e.Line == "" || !strings.Contains(e.Line, "45%") {
		t.Fatalf("no line from the child log: %+v", e)
	}
	if e.Progress == nil || *e.Progress < 0.44 || *e.Progress > 0.46 {
		t.Fatalf("progress = %v", e.Progress)
	}
}

// Two routers, two loads, independently: llamb can hold several models at once.
func TestLoadsArePerSlot(t *testing.T) {
	b := newLoadBoard("")
	b.observe("llama", map[string]string{"Big": "loading"})
	b.observe("llamb", map[string]string{"Small": "loading"})
	got := b.snapshot()
	if len(got) != 2 || findLoad(t, got, "llama", "Big") == nil || findLoad(t, got, "llamb", "Small") == nil {
		t.Fatalf("loads not per slot: %+v", got)
	}
}

// And it reaches the document the Android plugin reads.
func TestStatusCarriesLoads(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)
	d.loads.observe("llama", map[string]string{"M1": "loading"})

	rec := httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/status", nil))
	var doc struct {
		Loads []modelLoad `json:"loads"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(doc.Loads) != 1 || doc.Loads[0].Model != "M1" || doc.Loads[0].Phase != loadLoading {
		t.Fatalf("/status loads = %+v", doc.Loads)
	}
}

func TestMetricsAnswersEverySlot(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)
	rec := httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/estate/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var doc metricsDoc
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if doc.Version != metricsVersion || doc.SinceMS == 0 {
		t.Fatalf("version/since wrong: %+v", doc)
	}
	if len(doc.Slots) != len(d.slots) {
		t.Fatalf("%d slots in metrics, %d configured", len(doc.Slots), len(d.slots))
	}
	for _, s := range doc.Slots {
		if s.Runtime.SinceMS == 0 {
			t.Fatalf("slot %s has no runtime since", s.Name)
		}
	}
	if doc.Loads == nil || doc.History == nil {
		t.Fatalf("loads/history must be arrays, not null: %+v", doc)
	}
}

func TestMetricsCountRequests(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer up.Close()
	cfg := testConfig(t)
	cfg.llama = engineConfig{url: up.URL}
	d := newTestDemesne(t, cfg)

	rec := httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("proxied request failed: %d %s", rec.Code, rec.Body.String())
	}
	var doc metricsDoc
	rec = httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/estate/metrics", nil))
	_ = json.Unmarshal(rec.Body.Bytes(), &doc)
	for _, s := range doc.Slots {
		if s.Name != "llama" {
			continue
		}
		if s.Runtime.RequestsTotal != 1 {
			t.Fatalf("requests_total = %d", s.Runtime.RequestsTotal)
		}
		if s.Runtime.LastLatencyMS == nil {
			t.Fatal("no latency recorded")
		}
	}
}

func TestEventsStartWithASnapshot(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)
	srv := httptest.NewServer(d)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/estate/events")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type %q", ct)
	}
	br := bufio.NewReader(resp.Body)
	head, _ := br.ReadString('\n')
	kind, _ := br.ReadString('\n')
	data, _ := br.ReadString('\n')
	if !strings.HasPrefix(head, "id: ") || !strings.Contains(kind, "event: snapshot") ||
		!strings.Contains(data, `"type":"snapshot"`) {
		t.Fatalf("first frame is not a snapshot: %q %q %q", head, kind, data)
	}
	if !strings.Contains(data, `"version":1`) {
		t.Fatalf("snapshot carries no metrics doc: %q", data)
	}
}

func TestEventsResumeFromACursor(t *testing.T) {
	d := newEventHub()
	d.publish(eventFrame{Type: "transition", Transition: &transitionRecord{Slot: "llama", To: "stop"}})
	d.publish(eventFrame{Type: "load", Load: &modelLoad{Slot: "llama", Model: "M1", Phase: "loading"}})
	frames, ok := d.since(1)
	if !ok || len(frames) != 1 || frames[0].Type != "load" {
		t.Fatalf("resume after 1 gave %v (%v)", frames, ok)
	}
	if _, ok := d.since(0); !ok {
		t.Fatal("resume from the beginning should be possible while the ring holds it")
	}
}

func TestEventsDropRatherThanBlock(t *testing.T) {
	d := newEventHub()
	id, _ := d.subscribe()
	defer d.unsubscribe(id)
	done := make(chan struct{})
	go func() {
		for i := 0; i < 200; i++ {
			d.publish(eventFrame{Type: "load", Load: &modelLoad{Slot: "llama"}})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("publish blocked on a slow subscriber")
	}
}

// A failure line belongs to the model it names. On 2026-11-07 a missing Thinklite gguf made the
// router exit, and the 27B that was loading on the same slot was marked failed with it.
func TestALoadFailureIsAttributedToItsOwnModel(t *testing.T) {
	dir := t.TempDir()
	log := "srv  load:   ./Qwen3.8-27B-abliterated-120k.gguf\n" +
		"gguf_init_from_file: failed to open GGUF file './Thinklite-27B-Q4_K_M.gguf' (No such file or directory)\n" +
		"instance name=Thinklite-27B-Q4_K_M exited with status 1\n"
	if err := os.WriteFile(filepath.Join(dir, "llama.err"), []byte(log), 0o600); err != nil {
		t.Fatal(err)
	}
	b := newLoadBoard(dir)
	b.observe("llama", map[string]string{
		"Qwen3.8-27B-abliterated-120k": "loading",
		"Thinklite-27B-Q4_K_M":         "loading",
	})
	b.refreshLogs([]string{"llama"})
	got := b.snapshot()
	qwen := findLoad(t, got, "llama", "Qwen3.8-27B-abliterated-120k")
	think := findLoad(t, got, "llama", "Thinklite-27B-Q4_K_M")
	if qwen == nil || think == nil {
		t.Fatalf("entries missing: %+v", got)
	}
	if qwen.Phase != loadLoading {
		t.Fatalf("the 27B was marked %q by another model's failure", qwen.Phase)
	}
	if think.Phase != loadFailed || think.Error == nil || !strings.Contains(*think.Error, "Thinklite") {
		t.Fatalf("Thinklite should carry its own failure: %+v", think)
	}
	if !strings.Contains(think.Line, "Thinklite") {
		t.Fatalf("the failure line was not kept: %q", think.Line)
	}
}

// A cursor from a previous process cannot be honoured, and silence is the wrong answer: the
// stream restarts with a snapshot instead.
func TestEventsRestartOnACursorFromAPreviousProcess(t *testing.T) {
	d := newEventHub()
	d.publish(eventFrame{Type: "load", Load: &modelLoad{Slot: "llama", Model: "M1"}})
	d.publish(eventFrame{Type: "load", Load: &modelLoad{Slot: "llama", Model: "M2"}})
	if _, ok := d.since(200); ok {
		t.Fatal("a cursor from the future was accepted; the stream would go silent")
	}
}

// The live reload is llama.cpp's own: GET <router>/models?reload=1. There is no POST for it.
func TestSlotReloadAsksTheRouterToReload(t *testing.T) {
	seen := make(chan string, 1)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.URL.RequestURI()
		_, _ = io.WriteString(w, `{"data":[{"id":"A"},{"id":"B"}]}`)
	}))
	defer up.Close()
	cfg := testConfig(t)
	cfg.llama = engineConfig{url: up.URL, bin: "/usr/bin/llama-server"}
	d := newTestDemesne(t, cfg)

	n, err := d.reloadPreset(d.slotByName("llama"))
	if err != nil {
		t.Fatalf("reloadPreset: %v", err)
	}
	if n != 2 {
		t.Fatalf("models = %d", n)
	}
	if got := <-seen; got != "/models?reload=1" {
		t.Fatalf("asked for %q", got)
	}
}

// A slot that is not a router has no preset, and saying so beats dialling it.
func TestSlotReloadRefusesANonRouter(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)
	if _, err := d.reloadPreset(d.slotByName("sd")); err == nil {
		t.Fatal("reloadPreset accepted a non-router slot")
	}
}

// While the router is loading, /v1/models is blocked for tens of seconds; the child log is the
// only witness, and it must still produce a load entry.
func TestLoadsFallBackToTheChildLog(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer up.Close()
	cfg := testConfig(t)
	cfg.llama = engineConfig{url: up.URL, bin: "/usr/bin/llama-server"}
	d := newTestDemesne(t, cfg)

	log := "srv  load:   --model\nsrv  load:   ./M1.gguf\nload_tensors: loading model tensors\n"
	if err := os.WriteFile(filepath.Join(cfg.logDir, "llama.err"), []byte(log), 0o600); err != nil {
		t.Fatal(err)
	}
	d.pollLoads()
	got := d.loads.snapshot()
	e := findLoad(t, got, "llama", "M1")
	if e == nil || e.Phase != loadLoading {
		t.Fatalf("no load entry from the log alone: %+v", got)
	}
}
