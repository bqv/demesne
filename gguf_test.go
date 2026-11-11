package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestGGUFHeaderFacts(t *testing.T) {
	path := "/home/user/var/model/llm/Spark-X2.5-4B-Q4_K_M.gguf"
	if _, err := os.Stat(path); err != nil {
		t.Skip("no local gguf to read")
	}
	f, err := readGGUF(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("n_ctx=%d n_params=%d size=%d quant=%s", f.NCtx, f.NParams, f.SizeByte, f.Quant)
	if f.NCtx < 1024 || f.NParams < 1e9 || f.SizeByte < 1e9 || f.Quant == "" {
		t.Fatalf("implausible facts: %+v", f)
	}
}

func TestDeviceLabelsAreAClosedSet(t *testing.T) {
	cases := []tenantSpec{
		{name: "a", group: "gpu", vram: true, args: []string{"-ngl", "99"}},
		{name: "b", group: "gpu", vram: true, args: []string{"-ngl", "0"}},
		{name: "c", group: "aux", vram: true, args: []string{"-ngl", "99"}},
		{name: "d", group: "aux", vram: true, args: []string{}},
		{name: "e", vram: false, args: []string{}},
		{name: "f", group: "gpu", vram: true, args: []string{"--n-gpu-layers=48"}},
	}
	allowed := map[string]bool{"RX7900": true, "GT1030": true, "CPU": true}
	for _, sp := range cases {
		if got := cardLabel(sp); !allowed[got] {
			t.Fatalf("%s -> %q, outside the closed set", sp.name, got)
		}
	}
}

func TestConfiguredCtxComesFromTheCommandLine(t *testing.T) {
	if got := configuredCtx(tenantSpec{args: []string{"-c", "122880"}}); got != 122880 {
		t.Fatalf("ctx = %d", got)
	}
	if got := configuredCtx(tenantSpec{args: []string{"--ctx-size=4096"}}); got != 4096 {
		t.Fatalf("ctx = %d", got)
	}
}

func TestAliasIsPreferredOverTheModelID(t *testing.T) {
	sp := tenantSpec{name: "spark", models: []string{"Spark-X2.5-4B-Q4_K_M-256k"}, aliases: []string{"spark", "spark-4b"}}
	if got := firstAlias(sp); got != "spark" {
		t.Fatalf("alias = %q", got)
	}
	if got := firstAlias(tenantSpec{models: []string{"x"}}); got != "" {
		t.Fatalf("a tenant without aliases must not invent one: %q", got)
	}
}

// slots[] is gone: tenants[] is the only account of the estate, and a mirror that outlives its
// migration becomes a second source of truth nobody dares delete.
func TestStatusHasNoSlotsKey(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)
	rec := httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/status", nil))
	var body map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body["slots"]; ok {
		t.Fatal("slots[] is published again")
	}
	if _, ok := body["tenants"]; !ok {
		t.Fatal("tenants[] is missing")
	}
}
