// slot_test.go -- the lifecycle, without the engines.
//
// The children are this test binary re-exec'd: TestMain notices DEMESNE_TEST_CHILD and
// becomes a one-endpoint HTTP server that a slot can spawn, kill and restart. That is
// enough to test the parts that actually break -- spawn, readiness, idle eviction,
// supervisor restart, foreign-port refusal -- with no GPU, no models and no network.
package main

import (
	"context"
	"encoding/json"
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

	d.evict(sd, "the test wants a second child")
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
