package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// reservation is an external process's claim on one card's VRAM. It exists because the estate is not
// the only thing that wants the GPU: a perplexity run and a load cannot both guess and both win.
type reservation struct {
	ID        string `json:"id"`
	Device    string `json:"device"`
	Bytes     uint64 `json:"bytes"`
	Label     string `json:"label"`
	ExpiresMS int64  `json:"expires_ms,omitempty"`
	HeldMS    int64  `json:"held_ms"`
}

type reserveBoard struct {
	mu sync.Mutex
	m  map[string]*reservation
}

func newReserveBoard() *reserveBoard { return &reserveBoard{m: map[string]*reservation{}} }

func (b *reserveBoard) prune(now time.Time) {
	for id, r := range b.m {
		if r.ExpiresMS > 0 && now.UnixMilli() >= r.ExpiresMS {
			delete(b.m, id)
		}
	}
}

func (b *reserveBoard) total(device string) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prune(time.Now())
	var n uint64
	for _, r := range b.m {
		if r.Device == device {
			n += r.Bytes
		}
	}
	return n
}

func (b *reserveBoard) snapshot() []reservation {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prune(time.Now())
	out := make([]reservation, 0, len(b.m))
	for _, r := range b.m {
		out = append(out, *r)
	}
	return out
}

func (b *reserveBoard) add(r *reservation) { b.mu.Lock(); b.m[r.ID] = r; b.mu.Unlock() }

func (b *reserveBoard) release(id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.m[id]; !ok {
		return false
	}
	delete(b.m, id)
	return true
}

// holder names the largest reservation on a device, for a refusal that says who is in the way.
func (b *reserveBoard) holder(device string) (string, time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var best *reservation
	for _, r := range b.m {
		if r.Device != device {
			continue
		}
		if best == nil || r.Bytes > best.Bytes {
			best = r
		}
	}
	if best == nil {
		return "", time.Time{}
	}
	return fmt.Sprintf("%.1f GiB by %s", float64(best.Bytes)/(1<<30), best.Label), time.UnixMilli(best.ExpiresMS)
}

var reserveDevices = map[string]bool{"RX7900": true, "GT1030": true}

// freeVRAM is what is left on that card, measured rather than inferred: the AMD card through sysfs,
// the NVIDIA card through NVML's own tool because there is no sysfs equivalent.
func freeVRAM(device string) (uint64, error) {
	switch device {
	case "RX7900":
		used, err := vramUsed()
		if err != nil {
			return 0, err
		}
		total := vramTotal()
		if total <= used {
			return 0, nil
		}
		return total - used, nil
	case "GT1030":
		out, err := exec.Command("nvidia-smi", "--query-gpu=memory.free", "--format=csv,noheader,nounits").Output()
		if err != nil {
			return 0, err
		}
		mb, err := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
		if err != nil {
			return 0, err
		}
		return mb << 20, nil
	}
	return 0, fmt.Errorf("unknown device %q", device)
}

func newReserveID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return "rsv-" + hex.EncodeToString(b)
}

func (d *demesne) handleReserve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "demesne: POST required", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Bytes  uint64 `json:"bytes"`
		Label  string `json:"label"`
		Device string `json:"device"`
		TTLS   int64  `json:"ttl_s"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "demesne: "+err.Error(), http.StatusBadRequest)
		return
	}
	if !reserveDevices[req.Device] {
		http.Error(w, "demesne: device is required and must be RX7900 or GT1030 "+
			"(a reservation is a VRAM claim; the CPU has none, and host memory belongs to the cgroup fence)",
			http.StatusBadRequest)
		return
	}
	if req.Bytes == 0 {
		http.Error(w, "demesne: bytes is required", http.StatusBadRequest)
		return
	}
	if req.Label == "" {
		req.Label = "external"
	}
	// What can be made available: free now, plus what on-demand tenants on that card hold and would
	// give up. keep and resident are policy, and a reservation must not silently stop them.
	free, err := freeVRAM(req.Device)
	if err != nil {
		http.Error(w, "demesne: cannot measure "+req.Device+": "+err.Error(), http.StatusConflict)
		return
	}
	reclaimable, _ := d.reclaimableOn(req.Device)
	if free+reclaimable < req.Bytes {
		held := d.reserves.total(req.Device)
		http.Error(w, fmt.Sprintf("demesne: %s has %s free and %s reclaimable from on-demand tenants, "+
			"but the reservation wants %s; %s already reserved", req.Device,
			gib(free), gib(reclaimable), gib(req.Bytes), gib(held)), http.StatusConflict)
		return
	}
	stopped := d.stopOnDemandOn(req.Device)
	res := &reservation{ID: newReserveID(), Device: req.Device, Bytes: req.Bytes, Label: req.Label,
		HeldMS: time.Now().UnixMilli()}
	if req.TTLS > 0 {
		res.ExpiresMS = time.Now().Add(time.Duration(req.TTLS) * time.Second).UnixMilli()
	}
	d.reserves.add(res)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id": res.ID, "device": res.Device, "bytes": res.Bytes, "label": res.Label,
		"expires_ms": res.ExpiresMS, "stopped": stopped,
	})
}

func (d *demesne) handleReserveRelease(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete && r.Method != http.MethodPost {
		http.Error(w, "demesne: DELETE required", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/reserve/")
	if id == "" {
		http.Error(w, "demesne: no reservation id", http.StatusBadRequest)
		return
	}
	ok := d.reserves.release(id)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"released": ok, "id": id})
}

// reclaimableOn reports how much VRAM on that card is held by tenants a reservation may stop.
func (d *demesne) reclaimableOn(device string) (uint64, []*tenant) {
	var bytes uint64
	var out []*tenant
	for _, t := range d.tenants {
		up, _ := t.isUp()
		if !up || t.specOf().policy != policyOnDemand {
			continue
		}
		if deviceOf(t.specOf()) != device {
			continue
		}
		out = append(out, t)
		bytes += uint64(t.specOf().vramMB) << 20
	}
	return bytes, out
}

func (d *demesne) reclaimableBytes(ts []*tenant) uint64 {
	var n uint64
	for _, t := range ts {
		n += uint64(t.specOf().vramMB) << 20
	}
	return n
}

// stopOnDemandOn stops the on-demand tenants on a card and reports which ones, because a reservation
// that quietly took somebody's model away would be indistinguishable from a crash.
func (d *demesne) stopOnDemandOn(device string) []string {
	var stopped []string
	for _, t := range d.tenants {
		up, _ := t.isUp()
		if !up || t.specOf().policy != policyOnDemand {
			continue
		}
		if deviceOf(t.specOf()) != device {
			continue
		}
		d.evict(t, stopOperator, "a reservation on "+device+" wants the card")
		stopped = append(stopped, t.specOf().name)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		busy := false
		for _, t := range d.tenants {
			if up, _ := t.isUp(); up && deviceOf(t.specOf()) == device {
				busy = true
				break
			}
		}
		if !busy {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	return stopped
}

func deviceOf(spec tenantSpec) string { return cardLabel(spec) }

// reserveCommand is the client verb: hold a claim for the duration of a foreign command.
func reserveCommand(args []string, cfg config) {
	fs := flag.NewFlagSet("reserve", flag.ExitOnError)
	bytes := fs.String("bytes", "", "how much VRAM to hold, e.g. 1.7G or 1825361111")
	ttl := fs.Duration("ttl", 30*time.Minute, "release after this long even if the holder dies")
	device := fs.String("device", "", "RX7900 or GT1030")
	label := fs.String("label", "external", "who is holding it")
	rest := splitCommand(args)
	if err := fs.Parse(rest.args); err != nil {
		os.Exit(2)
	}
	n, err := parseBytes(*bytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "demesne: --bytes: %v\n", err)
		os.Exit(2)
	}
	body, _ := json.Marshal(map[string]any{"bytes": n, "label": *label, "device": *device,
		"ttl_s": int64(ttl.Seconds())})
	resp, errs := callBody(cfg, http.MethodPost, "/reserve", body)
	if resp == "" {
		fmt.Fprintf(os.Stderr, "demesne: reserve: %s\n", strings.Join(errs, "; "))
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "demesne: reserve: %v\n", err)
		os.Exit(1)
	}
	var got struct {
		ID      string   `json:"id"`
		Stopped []string `json:"stopped"`
	}
	if err := json.Unmarshal([]byte(resp), &got); err != nil || got.ID == "" {
		fmt.Fprintf(os.Stderr, "demesne: reserve: %s\n", strings.TrimSpace(resp))
		os.Exit(1)
	}
	if len(got.Stopped) > 0 {
		fmt.Fprintf(os.Stderr, "demesne: reserved %s (stopped %s)\n", got.ID, strings.Join(got.Stopped, ", "))
	} else {
		fmt.Fprintf(os.Stderr, "demesne: reserved %s\n", got.ID)
	}
	release := func() {
		_, _ = call(cfg, http.MethodDelete, "/reserve/"+got.ID)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	done := make(chan int, 1)
	go func() {
		cmd := exec.Command(rest.cmd, rest.args...)
		cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
		cmd.Env = append(os.Environ(), "DEMESNE_RESERVATION="+got.ID)
		if err := cmd.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "demesne: %v\n", err)
			done <- 127
			return
		}
		_ = cmd.Wait()
		done <- cmd.ProcessState.ExitCode()
	}()
	select {
	case code := <-done:
		release()
		os.Exit(code)
	case s := <-sig:
		_ = s
		release()
		os.Exit(143)
	}
}

type commandTail struct {
	cmd  string
	args []string
}

func splitCommand(args []string) commandTail {
	for i, a := range args {
		if a == "--" {
			if i+1 < len(args) {
				return commandTail{cmd: args[i+1], args: args[i+2:]}
			}
			return commandTail{}
		}
	}
	return commandTail{}
}

func parseBytes(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty")
	}
	mult := uint64(1)
	switch s[len(s)-1] {
	case 'k', 'K':
		mult, s = 1<<10, s[:len(s)-1]
	case 'm', 'M':
		mult, s = 1<<20, s[:len(s)-1]
	case 'g', 'G':
		mult, s = 1<<30, s[:len(s)-1]
	}
	n, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	return uint64(n * float64(mult)), nil
}
