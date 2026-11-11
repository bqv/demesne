package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const defaultReadMBs = 519.0

const (
	keyKind      = "kind"
	keyModels    = "models"
	keyAliases   = "aliases"
	keyURL       = "url"
	keyBin       = "bin"
	keyArgs      = "args"
	keyEnv       = "env"
	keyDir       = "dir"
	keyPolicy    = "policy"
	keyEnabled   = "enabled"
	keyGroup     = "group"
	keyVRAM      = "vram_mb"
	keyIdle      = "idle"
	keyMemHigh   = "memory_high"
	keyMemMax    = "memory_max"
	keyMemSwap   = "memory_swap_high"
	keyReadMBs   = "read_mb_s"
	keyFallback  = "fallback"
	keyModelsBad = "model"
)

var tenantKeys = map[string]bool{
	keyKind: true, keyModels: true, keyAliases: true, keyURL: true, keyBin: true,
	keyArgs: true, keyEnv: true, keyDir: true, keyPolicy: true, keyGroup: true,
	keyVRAM: true, keyIdle: true, keyMemHigh: true, keyMemMax: true, keyMemSwap: true,
	keyReadMBs: true, keyFallback: true, keyModelsBad: true, keyEnabled: true,
}

type tenantSection struct {
	name string
	kv   map[string]string
	line int
}

func parseTenantsFile(path string) ([]tenantSection, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []tenantSection
	cur := -1
	for i, raw := range strings.Split(string(b), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") {
			if !strings.HasSuffix(line, "]") {
				return nil, fmt.Errorf("%s:%d: unterminated section header %q", path, i+1, line)
			}
			name := strings.TrimSpace(line[1 : len(line)-1])
			if name == "" {
				return nil, fmt.Errorf("%s:%d: empty section name", path, i+1)
			}
			for _, prev := range out {
				if prev.name == name {
					return nil, fmt.Errorf("%s:%d: duplicate tenant %q", path, i+1, name)
				}
			}
			out = append(out, tenantSection{name: name, kv: map[string]string{}, line: i + 1})
			cur = len(out) - 1
			continue
		}
		if cur < 0 {
			return nil, fmt.Errorf("%s:%d: %q is outside any [tenant]", path, i+1, line)
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("%s:%d: %q is not key = value", path, i+1, line)
		}
		key = strings.ToLower(strings.TrimSpace(key))
		value = strings.TrimSpace(value)
		if !tenantKeys[key] {
			return nil, fmt.Errorf("%s:%d: unknown key %q in [%s]", path, i+1, key, out[cur].name)
		}
		if key == keyModelsBad {
			return nil, fmt.Errorf("%s:%d: use %q, not %q, in [%s] -- a tenant serves a set",
				path, i+1, keyModels, keyModelsBad, out[cur].name)
		}
		out[cur].kv[key] = value
	}
	return out, nil
}

func specFromSection(sec tenantSection, c config) (tenantSpec, error) {
	kv := sec.kv
	kind, err := parseTenantKind(kv[keyKind])
	if err != nil {
		return tenantSpec{}, fmt.Errorf("%s: %w", sec.name, err)
	}
	url := kv[keyURL]
	if url == "" {
		return tenantSpec{}, fmt.Errorf("%s: url is required", sec.name)
	}
	pol, err := parsePolicy(kv[keyPolicy])
	if err != nil {
		return tenantSpec{}, fmt.Errorf("%s: %w", sec.name, err)
	}
	models := splitNames(kv[keyModels])
	aliases := splitNames(kv[keyAliases])
	if kind == kindModel && len(models) == 0 && len(aliases) == 0 {
		return tenantSpec{}, fmt.Errorf("%s: kind=model needs models or aliases, or nothing can address it", sec.name)
	}
	spec := tenantSpec{
		name: sec.name, url: url, bin: kv[keyBin], args: fields(kv[keyArgs]),
		env: fields(kv[keyEnv]), dir: kv[keyDir], models: models, aliases: aliases,
		group: kv[keyGroup], policy: pol, kind: kind, cgroup: c.memLimits(sec.name, kv),
	}
	if s := kv[keyIdle]; s != "" {
		d, err := time.ParseDuration(s)
		if err != nil || d < 0 {
			return tenantSpec{}, fmt.Errorf("%s: idle %q is not a duration", sec.name, s)
		}
		spec.idle = d
	} else if pol == policyOnDemand {
		spec.idle = c.idle
	}
	if s := kv[keyVRAM]; s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 {
			return tenantSpec{}, fmt.Errorf("%s: vram_mb %q is not a number", sec.name, s)
		}
		spec.vramMB = n
	}
	if s := kv[keyReadMBs]; s != "" {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil || f <= 0 {
			return tenantSpec{}, fmt.Errorf("%s: read_mb_s %q is not a positive number", sec.name, s)
		}
		spec.readMBs = f
	}
	spec.vram = spec.group != ""
	spec.router = kind == kindModel
	spec.routed = true
	spec.fallback = strings.EqualFold(kv[keyFallback], "true")
	if path := unixPath(url); url != "" && strings.HasPrefix(url, "unix:") {
		if path == "" || !filepath.IsAbs(path) {
			return tenantSpec{}, fmt.Errorf("%s: unix url needs an absolute path, got %q", sec.name, url)
		}
		if !strings.HasSuffix(path, ".sock") {
			return tenantSpec{}, fmt.Errorf("%s: %q must end in .sock or the engine treats it as a TCP host", sec.name, path)
		}
	}
	return spec, nil
}

func splitNames(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t'
	})
}

func parsePolicy(s string) (policy, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "on-demand", "ondemand":
		return policyOnDemand, nil
	case "external":
		return policyExternal, nil
	case "resident":
		return policyResident, nil
	case "warm":
		return 0, fmt.Errorf("policy %q is retired: a tenant that must stay up is %q, and the big card keeps nothing resident", s, "resident")
	}
	return 0, fmt.Errorf("unknown policy %q (on-demand or external)", s)
}

func (c config) memLimits(tenant string, kv map[string]string) memLimit {
	m := memLimitFor(c.cgroupRoot, tenant, envRaw)
	if m.leaf == "" {
		m.leaf = filepath.Join(c.cgroupRoot, cgroupLeaf(tenant))
	}
	if v := kv[keyMemHigh]; v != "" {
		m.high = v
	}
	if v := kv[keyMemMax]; v != "" {
		m.max = v
	}
	if v := kv[keyMemSwap]; v != "" {
		m.swapHigh = v
	}
	return m
}

func tenantsFromConfig(c config) ([]tenantSpec, []string, error) {
	base := buildTenants(c)
	if c.tenantsFile == "" {
		return base, nil, nil
	}
	path := c.tenantsFile
	sections, err := parseTenantsFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("tenants file: %w", err)
	}
	byName := map[string]tenantSpec{}
	for _, sp := range base {
		byName[sp.name] = sp
	}
	var notes []string
	for _, sec := range sections {
		if off, ok := sec.kv[keyEnabled]; ok && !strings.EqualFold(off, "true") && off != "" {
			delete(byName, sec.name)
			notes = append(notes, fmt.Sprintf("%s: disabled by %s", sec.name, path))
			continue
		}
		sp, err := specFromSection(sec, c)
		if err != nil {
			return nil, nil, err
		}
		if old, ok := byName[sp.name]; ok {
			notes = append(notes, fmt.Sprintf("%s: defined in %s, replacing the built-in default (%s %s)",
				sp.name, path, old.policy, old.url))
		}
		byName[sp.name] = sp
	}
	out := make([]tenantSpec, 0, len(byName))
	for _, sp := range byName {
		out = append(out, sp)
	}
	return sortedTenants(out), notes, nil
}

func sortedTenants(in []tenantSpec) []tenantSpec {
	out := append([]tenantSpec(nil), in...)
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

func sortedTenantPtrs(in []*tenant) []*tenant {
	out := append([]*tenant(nil), in...)
	sort.Slice(out, func(i, j int) bool { return out[i].specOf().name < out[j].specOf().name })
	return out
}

func runCheck(cfg config) {
	specs, notes, err := tenantsFromConfig(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "demesne: %v\n", err)
		os.Exit(1)
	}
	for _, note := range notes {
		fmt.Println("note: " + note)
	}
	for _, sp := range sortedTenants(specs) {
		fmt.Println(tenantLogLine(sp))
	}
	if len(notes) > 0 {
		fmt.Printf("%d tenant(s) replaced a built-in default\n", len(notes))
	}
}

func tenantLogLine(sp tenantSpec) string {
	return fmt.Sprintf("tenant %-10s kind=%-6s policy=%-9s group=%-4s url=%-42s models=%-34s vram_mb=%-7s reload_s=%-5s idle=%s",
		sp.name, sp.kind, sp.policy, orNone(sp.group), sp.url, strings.Join(append(append([]string{}, sp.models...), sp.aliases...), ","),
		vramDeclared(sp), reloadCost(sp), idleString(sp))
}

func vramDeclared(sp tenantSpec) string {
	if sp.vramMB == 0 {
		return "?"
	}
	return fmt.Sprintf("%d", sp.vramMB)
}

func idleString(sp tenantSpec) string {
	if sp.idle == 0 {
		return "never"
	}
	return sp.idle.String()
}

func reloadCost(sp tenantSpec) string {
	n := weightsBytes(sp)
	if n == 0 {
		return "?"
	}
	secs := float64(n) / (defaultReadMBs * 1e6)
	return fmt.Sprintf("%.0f", secs)
}

func reloadSeconds(sp tenantSpec) string {
	n := weightsBytes(sp)
	if n == 0 {
		return "?"
	}
	return fmt.Sprintf("%.0fs", float64(n)/(defaultReadMBs*1e6))
}

func weightsBytes(sp tenantSpec) int64 {
	path := modelPath(sp)
	if path == "" {
		return 0
	}
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}

func modelPathFor(sp tenantSpec) string { return modelPath(sp) }

func modelPath(sp tenantSpec) string {
	args := sp.args
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-m", "--model":
			if i+1 < len(args) {
				return resolveModelPath(sp.dir, args[i+1])
			}
		}
		if strings.HasPrefix(args[i], "--model=") {
			return resolveModelPath(sp.dir, strings.TrimPrefix(args[i], "--model="))
		}
	}
	return ""
}

func resolveModelPath(dir, path string) string {
	if filepath.IsAbs(path) || dir == "" {
		return path
	}
	return filepath.Join(dir, path)
}
