package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func goldenConfig() config {
	return config{
		sd: engineConfig{url: "http://127.0.0.1:55558", bin: "/bin/sd", args: []string{"--sd"},
			env: []string{"SD=1"}, dir: "/srv/sd",
			cgroup: memLimit{leaf: "/sys/fs/cgroup/estate/sd"}},
		llama: engineConfig{url: "http://127.0.0.1:55556", bin: "/bin/llama", args: []string{"--llama"},
			models: []string{"m1"}, dir: "/srv/llm",
			cgroup: memLimit{leaf: "/sys/fs/cgroup/estate/llama/llm"}},
		llamb: engineConfig{url: "http://127.0.0.1:55557", bin: "/bin/llamb", args: []string{"--llamb"},
			cgroup: memLimit{leaf: "/sys/fs/cgroup/estate/llama/aux"}},
		whisper: engineConfig{url: "http://127.0.0.1:55559", bin: "/bin/whisper",
			args:   []string{"--whisper"},
			cgroup: memLimit{high: "1G", max: "2G", leaf: "/sys/fs/cgroup/estate/whisper"}},
		idle: 30 * time.Minute, whisperIdle: 10 * time.Minute,
		cgroupRoot: "/sys/fs/cgroup/estate",
	}
}

func TestTheFiveDefaultsAreUnchanged(t *testing.T) {
	cfg := goldenConfig()
	got := sortedTenants(buildTenants(cfg))
	want := []tenantSpec{
		{
			name: "llama", url: "http://127.0.0.1:55556", bin: "/bin/llama", args: []string{"--llama"},
			dir: "/srv/llm", models: []string{"m1"}, kind: kindModel, boot: true, group: "gpu", vram: true,
			policy: policyResident, routed: true, router: true, fallback: true,
			cgroup: memLimit{leaf: "/sys/fs/cgroup/estate/llama/llm"},
		},
		{
			name: "llamb", url: "http://127.0.0.1:55557", bin: "/bin/llamb", args: []string{"--llamb"},
			kind: kindModel, boot: true, group: "aux", vram: true, policy: policyResident,
			routed: true, router: true, fallback: true,
			cgroup: memLimit{leaf: "/sys/fs/cgroup/estate/llama/aux"},
		},
		{
			name: "sd", url: "http://127.0.0.1:55558", bin: "/bin/sd", args: []string{"--sd"},
			env: []string{"SD=1"}, dir: "/srv/sd", kind: kindMedia, group: "gpu", vram: true,
			policy: policyOnDemand, idle: 30 * time.Minute, routed: true,
			cgroup: memLimit{leaf: "/sys/fs/cgroup/estate/sd"},
		},
		{
			name: "whisper", url: "http://127.0.0.1:55559", bin: "/bin/whisper",
			args: []string{"--whisper"}, kind: kindAudio, policy: policyOnDemand,
			idle: 10 * time.Minute, routed: true,
			cgroup: memLimit{high: "1G", max: "2G", leaf: "/sys/fs/cgroup/estate/whisper"},
		},
	}
	if len(got) != len(want) {
		t.Fatalf("tenants = %d, want %d (strata is absent without a url)", len(got), len(want))
	}
	for i := range want {
		if got[i].name != want[i].name {
			t.Fatalf("tenant %d = %q, want %q", i, got[i].name, want[i].name)
		}
		if !reflect.DeepEqual(got[i], want[i]) {
			t.Fatalf("tenant %s:\n got %+v\nwant %+v", want[i].name, got[i], want[i])
		}
	}
}

func writeTenants(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tenants.ini")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestATenantFileDefinesAndOverrides(t *testing.T) {
	cfg := goldenConfig()
	cfg.tenantsFile = writeTenants(t, `
[llama]
url = http://127.0.0.1:55556
models = m1, m2
vram_mb = 19000
idle = 45m

[spark]
url = unix:/home/user/run/models/spark.sock
bin = /usr/bin/llama-server
args = -m /srv/model/llm/Spark.gguf --alias spark --host /home/user/run/models/spark.sock
models = spark
aliases = spark-4b
group = gpu
vram_mb = 3200
idle = 5m
`)
	specs, notes, err := tenantsFromConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(notes) != 1 || !strings.Contains(notes[0], "llama") || !strings.Contains(notes[0], "replacing the built-in default") {
		t.Fatalf("override note = %v", notes)
	}
	byName := map[string]tenantSpec{}
	for _, sp := range specs {
		byName[sp.name] = sp
	}
	if got := byName["llama"]; got.vramMB != 19000 || got.idle != 45*time.Minute || len(got.models) != 2 {
		t.Fatalf("llama was not overridden: %+v", got)
	}
	spark, ok := byName["spark"]
	if !ok {
		t.Fatal("the new tenant was not added")
	}
	if spark.kind != kindModel || !spark.router || spark.policy != policyOnDemand ||
		spark.idle != 5*time.Minute || spark.vramMB != 3200 || len(spark.aliases) != 1 {
		t.Fatalf("spark = %+v", spark)
	}
	if spark.cgroup.leaf != "/sys/fs/cgroup/estate/spark" {
		t.Fatalf("spark leaf = %q", spark.cgroup.leaf)
	}
	for i := 1; i < len(specs); i++ {
		if specs[i-1].name > specs[i].name {
			t.Fatalf("tenants are not in name order: %v then %v", specs[i-1].name, specs[i].name)
		}
	}
}

func TestATenantsFileRefusesWhatWouldLie(t *testing.T) {
	cases := map[string]string{
		"warm policy": `[a]
url = http://127.0.0.1:1
policy = warm
`,
		"unknown key": `[a]
url = http://127.0.0.1:1
nonsense = 1
`,
		"model with no names": `[a]
url = http://127.0.0.1:1
`,
		"relative unix path": `[a]
url = unix:models/a.sock
models = a
`,
		"unix path without .sock": `[a]
url = unix:/run/a.socket
models = a
`,
		"duplicate tenant": `[a]
url = http://127.0.0.1:1
models = a
[a]
url = http://127.0.0.1:2
models = b
`,
		"key outside a tenant": `url = http://127.0.0.1:1
`,
	}
	for name, body := range cases {
		cfg := goldenConfig()
		cfg.tenantsFile = writeTenants(t, body)
		if _, _, err := tenantsFromConfig(cfg); err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
}

func TestTheTenantLogIsInNameOrder(t *testing.T) {
	sp := []tenantSpec{
		{name: "whisper", url: "http://127.0.0.1:3", policy: policyExternal},
		{name: "llama", url: "http://127.0.0.1:1", policy: policyExternal},
	}
	sorted := sortedTenants(sp)
	lines := []string{tenantLogLine(sorted[0]), tenantLogLine(sorted[1])}
	if !strings.HasPrefix(lines[0], "tenant llama ") || !strings.HasPrefix(lines[1], "tenant whisper ") {
		t.Fatalf("log order = %q then %q", lines[0], lines[1])
	}
	if !strings.Contains(lines[0], "vram_mb=?") || !strings.Contains(lines[0], "reload_s=?") {
		t.Fatalf("an undeclared tenant should say so: %q", lines[0])
	}
}

// A per-model tenant cannot report its own load state: its engine answers /v1/models with one
// model and nothing else, so the board is told by the side that asked for the spawn.
func TestAPerModelTenantGetsALoadRow(t *testing.T) {
	b := newLoadBoard(t.TempDir())
	b.noteLoading("spark", "spark")
	snap := b.snapshot()
	if len(snap) != 1 || snap[0].Phase != loadLoading || snap[0].Tenant != "spark" {
		t.Fatalf("after noteLoading: %+v", snap)
	}
	b.notePhase("spark", "spark", loadReady, nil)
	snap = b.snapshot()
	if len(snap) != 1 || snap[0].Phase != loadReady || snap[0].FinishedMS == nil {
		t.Fatalf("after notePhase(ready): %+v", snap)
	}
	b.noteLoading("spark", "spark")
	b.notePhase("spark", "spark", loadFailed, errors.New("the child exited while starting"))
	snap = b.snapshot()
	if snap[0].Phase != loadFailed || snap[0].Error == nil {
		t.Fatalf("after notePhase(failed): %+v", snap)
	}
}

// Placement is a fact about where the weights run, not about which scheduling group the tenant
// belongs to: a -ngl 0 tenant is on the CPU even when it is in the gpu group, and a field that
// says RX7900 about it is worse than no field.
func TestPlacementFollowsTheCommandLine(t *testing.T) {
	cases := []struct {
		spec tenantSpec
		want string
	}{
		{tenantSpec{group: "gpu", vram: true, args: []string{"-ngl", "0"}}, "CPU"},
		{tenantSpec{group: "gpu", vram: true, args: []string{"-ngl", "99", "-c", "4096"}}, "RX7900"},
		{tenantSpec{group: "aux", vram: true, args: []string{"--n-gpu-layers=99"}}, "GT1030"},
		{tenantSpec{group: "gpu", vram: true, args: []string{"--models-dir", "/x"}}, "RX7900"},
		{tenantSpec{args: []string{"--model", "/x.bin"}}, "CPU"},
	}
	for _, c := range cases {
		if got := cardLabel(c.spec); got != c.want {
			t.Fatalf("cardLabel(%v) = %q, want %q", c.spec.args, got, c.want)
		}
	}
}

// A cold tenant's declared models and aliases must be in /v1/models before its first request,
// or a picker cannot offer what it cannot see.
func TestAColdTenantIsListed(t *testing.T) {
	cfg := testConfig(t)
	cfg.llama = engineConfig{url: "http://127.0.0.1:1"}
	d := newTestDemesne(t, cfg)
	sparkSpec := tenantSpec{
		name: "spark", url: "unix:/tmp/spark.sock", kind: kindModel, models: []string{"Spark-X2.5"},
		aliases: []string{"spark"}, group: "gpu", policy: policyOnDemand, routed: true, router: true,
	}
	sp := &tenant{spec: sparkSpec, owner: d}
	d.tenants = append(d.tenants, sp)
	d.index["spark"] = sp

	rec := httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	var body struct {
		Data []struct {
			ID      string `json:"id"`
			OwnedBy string `json:"owned_by"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("%v: %s", err, rec.Body.String())
	}
	got := map[string]string{}
	for _, m := range body.Data {
		got[m.ID] = m.OwnedBy
	}
	if got["spark"] != "RX7900" || got["Spark-X2.5"] != "RX7900" {
		t.Fatalf("a cold tenant's names are missing or mislabelled: %v", got)
	}
}

// A built-in can be dropped, which is how a router is retired once its models are tenants of
// their own; and a tenant may be resident, which is how two models cohabit a small card without
// an idle timer deciding to evict the one recall is using.
func TestATenantCanReplaceAndDropABuiltIn(t *testing.T) {
	cfg := goldenConfig()
	cfg.tenantsFile = writeTenants(t, `
[llamb]
enabled = false

[embed]
url = http://127.0.0.1:55562
models = Qwen3-Embedding-0.6B-Q8_0
group = aux
policy = resident
vram_mb = 700
`)
	specs, notes, err := tenantsFromConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, sp := range specs {
		if sp.name == "llamb" {
			t.Fatal("a disabled built-in is still in the estate")
		}
	}
	var embed *tenantSpec
	for i := range specs {
		if specs[i].name == "embed" {
			embed = &specs[i]
		}
	}
	if embed == nil {
		t.Fatal("the resident tenant was not added")
	}
	if embed.policy != policyResident || embed.vramMB != 700 || embed.group != "aux" {
		t.Fatalf("embed = %+v", embed)
	}
	found := false
	for _, n := range notes {
		if strings.Contains(n, "llamb: disabled") {
			found = true
		}
	}
	if !found {
		t.Fatalf("dropping a built-in was not noted: %v", notes)
	}
}

// start separates "is up from boot" from "how eviction treats it": the big card's rungs are keep
// (never idle-evicted) but must not all boot at once, so they start on demand.
func TestStartIsNotTheSameAsPolicy(t *testing.T) {
	cfg := goldenConfig()
	cfg.tenantsFile = writeTenants(t, `
[a]
url = http://127.0.0.1:1
models = a
group = gpu
policy = keep
start = demand

[b]
url = http://127.0.0.1:2
models = b
group = aux
policy = keep
start = boot

[c]
url = http://127.0.0.1:3
models = c
policy = resident
`)
	specs, _, err := tenantsFromConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, sp := range specs {
		switch sp.name {
		case "a", "b", "c":
			got[sp.name] = sp.boot
		}
	}
	if got["a"] || !got["b"] || !got["c"] {
		t.Fatalf("boot flags = %v (a must be demand, b demand->boot, c resident->boot)", got)
	}
}
