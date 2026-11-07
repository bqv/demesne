package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const metricsVersion = 1

const historyLen = 32

type transitionRecord struct {
	AtMS   int64  `json:"at_ms"`
	Slot   string `json:"slot"`
	Kind   string `json:"kind"`
	To     string `json:"to"`
	Reason string `json:"reason,omitempty"`
}

type history struct {
	mu   sync.Mutex
	ring []transitionRecord
}

func (h *history) record(r transitionRecord) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ring = append(h.ring, r)
	if len(h.ring) > historyLen {
		h.ring = h.ring[len(h.ring)-historyLen:]
	}
}

func (h *history) snapshot() []transitionRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]transitionRecord, len(h.ring))
	copy(out, h.ring)
	return out
}

func (d *demesne) recordTransition(slot, kind, to, reason string) {
	r := transitionRecord{AtMS: time.Now().UnixMilli(), Slot: slot, Kind: kind, To: to, Reason: reason}
	d.history.record(r)
	d.hub.publish(eventFrame{Type: "transition", AtMS: r.AtMS, Transition: &r})
}

type slotRuntime struct {
	SinceMS       int64    `json:"since_ms"`
	RequestsTotal int64    `json:"requests_total"`
	Active        int64    `json:"active"`
	ErrorsTotal   int64    `json:"errors_total"`
	LastLatencyMS *float64 `json:"last_latency_ms"`
}

type cgroupStat struct {
	CurrentBytes int64  `json:"current_bytes"`
	HighBytes    *int64 `json:"high_bytes"`
	MaxBytes     *int64 `json:"max_bytes"`
	OOMKills     int64  `json:"oom_kills"`
}

type slotMetrics struct {
	Name           string      `json:"name"`
	URL            string      `json:"url"`
	Port           int         `json:"port"`
	Policy         string      `json:"policy"`
	Group          string      `json:"group"`
	Up             bool        `json:"up"`
	Booting        bool        `json:"booting"`
	PID            *int        `json:"pid"`
	Foreign        bool        `json:"foreign"`
	UptimeS        *float64    `json:"uptime_s"`
	Restarts       int         `json:"restarts"`
	InFlight       int64       `json:"in_flight"`
	LastTransition string      `json:"last_transition,omitempty"`
	LastError      *string     `json:"last_error"`
	Load           *modelLoad  `json:"load"`
	Runtime        slotRuntime `json:"runtime"`
	Cgroup         *cgroupStat `json:"cgroup"`
}

type metricsDoc struct {
	Version       int                 `json:"version"`
	SinceMS       int64               `json:"since_ms"`
	UptimeS       float64             `json:"uptime_s"`
	Listen        string              `json:"listen"`
	Socket        string              `json:"socket"`
	IdleTimeoutS  float64             `json:"idle_timeout_s"`
	MediaInFlight int64               `json:"media_in_flight"`
	VramUsedGiB   float64             `json:"vram_used_gib"`
	VramTotalGiB  float64             `json:"vram_total_gib"`
	VramFreeGiB   float64             `json:"vram_free_gib"`
	Models        map[string][]string `json:"models"`
	Owner         map[string]string   `json:"owner"`
	Unavailable   []string            `json:"unavailable"`
	Slots         []slotMetrics       `json:"slots"`
	Loads         []modelLoad         `json:"loads"`
	History       []transitionRecord  `json:"history"`
}

func vramTotal() uint64 {
	totals, err := filepath.Glob("/sys/class/drm/card*/device/mem_info_vram_total")
	if err != nil {
		return 0
	}
	var biggest uint64
	for _, p := range totals {
		if t, err := readUint(p); err == nil && t > biggest {
			biggest = t
		}
	}
	return biggest
}

func readCgroup(leaf string) *cgroupStat {
	if leaf == "" {
		return nil
	}
	dir := strings.TrimSuffix(leaf, "/")
	cur, err := readInt64(filepath.Join(dir, "memory.current"))
	if err != nil {
		return nil
	}
	out := &cgroupStat{CurrentBytes: cur}
	if v, err := readInt64(filepath.Join(dir, "memory.high")); err == nil && v > 0 {
		out.HighBytes = &v
	}
	if v, err := readInt64(filepath.Join(dir, "memory.max")); err == nil {
		out.MaxBytes = &v
	}
	if b, err := os.ReadFile(filepath.Join(dir, "memory.events")); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "oom_kill ") {
				if n, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, "oom_kill ")), 10, 64); err == nil {
					out.OOMKills = n
				}
			}
		}
	}
	return out
}

func readInt64(path string) (int64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
}

func (d *demesne) metrics() metricsDoc {
	used, _ := vramUsed()
	total := vramTotal()
	doc := metricsDoc{
		Version:      metricsVersion,
		SinceMS:      d.startedAt.UnixMilli(),
		UptimeS:      time.Since(d.startedAt).Seconds(),
		Listen:       d.cfg.listen,
		Socket:       d.cfg.socket,
		IdleTimeoutS: d.cfg.idle.Seconds(),
		Models:       map[string][]string{},
		Owner:        map[string]string{},
		Unavailable:  []string{"tokens_in", "tokens_out", "prefill_tps", "decode_tps", "queue_depth", "wait_ms"},
	}
	if sd := d.slotByName("sd"); sd != nil {
		doc.MediaInFlight = atomic.LoadInt64(&sd.inflight)
	}
	doc.VramUsedGiB = float64(used) / (1 << 30)
	doc.VramTotalGiB = float64(total) / (1 << 30)
	if doc.VramTotalGiB > doc.VramUsedGiB {
		doc.VramFreeGiB = doc.VramTotalGiB - doc.VramUsedGiB
	}

	byModel, _ := d.modelsSnapshot()
	for model, slot := range byModel {
		doc.Owner[model] = slot
		doc.Models[slot] = append(doc.Models[slot], model)
	}
	for slot := range doc.Models {
		sortStrings(doc.Models[slot])
	}

	for _, s := range d.slots {
		spec := s.specOf()
		st := slotStatusOf(s)
		m := slotMetrics{
			Name:           spec.name,
			URL:            spec.url,
			Port:           s.port(),
			Policy:         spec.policy.String(),
			Group:          spec.group,
			Up:             st.Up,
			Booting:        st.Booting,
			Foreign:        st.Foreign != "",
			Restarts:       st.Restarts,
			InFlight:       st.InFlight,
			LastTransition: string(st.LastTransition),
			Runtime: slotRuntime{
				SinceMS:       d.startedAt.UnixMilli(),
				RequestsTotal: s.requests.Load(),
				Active:        s.active.Load(),
				ErrorsTotal:   s.errors.Load(),
			},
			Cgroup: readCgroup(spec.cgroup.leaf),
		}
		if st.PID != 0 {
			pid := st.PID
			m.PID = &pid
		}
		if st.LastError != "" {
			e := st.LastError
			m.LastError = &e
		}
		if !s.readySince().IsZero() {
			u := time.Since(s.readySince()).Seconds()
			m.UptimeS = &u
		}
		if us := s.lastLatency.Load(); us > 0 {
			ms := float64(us) / 1000
			m.Runtime.LastLatencyMS = &ms
		}
		if l := latestLoad(d.loads.snapshot(), spec.name); l != nil {
			m.Load = l
		}
		m.Cgroup = readCgroup(spec.cgroup.leaf)
		doc.Slots = append(doc.Slots, m)
	}
	doc.Loads = d.loads.snapshot()
	doc.History = d.history.snapshot()
	return doc
}

func latestLoad(loads []modelLoad, slot string) *modelLoad {
	var out *modelLoad
	for i := range loads {
		if loads[i].Slot != slot {
			continue
		}
		if out == nil || loads[i].StartedMS > out.StartedMS {
			out = &loads[i]
		}
	}
	return out
}

func sortStrings(v []string) {
	for i := 1; i < len(v); i++ {
		for j := i; j > 0 && v[j] < v[j-1]; j-- {
			v[j], v[j-1] = v[j-1], v[j]
		}
	}
}

func (d *demesne) modelsSnapshot() (map[string]string, map[string]int) {
	d.models.mu.Lock()
	defer d.models.mu.Unlock()
	by := make(map[string]string, len(d.models.byModel))
	for k, v := range d.models.byModel {
		by[k] = v
	}
	loaded := make(map[string]int, len(d.models.loaded))
	for k, v := range d.models.loaded {
		loaded[k] = v
	}
	return by, loaded
}

func (d *demesne) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "demesne: GET required", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(d.metrics())
}
