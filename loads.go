package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	loadQueued  = "queued"
	loadLoading = "loading"
	loadReady   = "ready"
	loadFailed  = "failed"
)

var errNotRouter = errors.New("not a llama.cpp router")

var (
	loadReadyFor  = 60 * time.Second
	loadFailedFor = 10 * time.Minute
	loadQueuedFor = 5 * time.Minute
)

type modelLoad struct {
	Slot       string   `json:"slot"`
	Model      string   `json:"model"`
	Phase      string   `json:"phase"`
	StartedMS  int64    `json:"started_ms"`
	FinishedMS *int64   `json:"finished_ms"`
	Error      *string  `json:"error"`
	Progress   *float64 `json:"progress"`
	Line       string   `json:"line"`
}

type loadBoard struct {
	mu       sync.Mutex
	entries  map[string]*modelLoad
	loaded   map[string]map[string]bool
	logDir   string
	onChange func(modelLoad)
}

func newLoadBoard(logDir string) *loadBoard {
	return &loadBoard{
		entries: map[string]*modelLoad{},
		loaded:  map[string]map[string]bool{},
		logDir:  logDir,
	}
}

func loadKey(slot, model string) string { return slot + "\x00" + model }

func (b *loadBoard) noteRequest(slot, model string) {
	if model == "" {
		return
	}
	b.mu.Lock()
	if b.loaded[slot][model] {
		b.mu.Unlock()
		return
	}
	if _, ok := b.entries[loadKey(slot, model)]; ok {
		b.mu.Unlock()
		return
	}
	entry := &modelLoad{
		Slot: slot, Model: model, Phase: loadQueued, StartedMS: time.Now().UnixMilli(),
	}
	b.entries[loadKey(slot, model)] = entry
	b.mu.Unlock()
	b.fire(*entry)
}

func (b *loadBoard) fire(l modelLoad) {
	if b.onChange != nil {
		b.onChange(l)
	}
}

func (b *loadBoard) observe(slot string, models map[string]string) {
	var changed []modelLoad
	defer func() {
		for _, l := range changed {
			b.fire(l)
		}
	}()
	now := time.Now().UnixMilli()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.loaded[slot] == nil {
		b.loaded[slot] = map[string]bool{}
	}
	for model, value := range models {
		has := b.loaded[slot][model]
		switch value {
		case "loaded":
			if !has {
				if e, ok := b.entries[loadKey(slot, model)]; ok && e.Phase != loadFailed {
					e.Phase, e.FinishedMS = loadReady, &now
					changed = append(changed, *e)
				}
				b.loaded[slot][model] = true
			}
		case "loading":
			b.loaded[slot][model] = false
			e, ok := b.entries[loadKey(slot, model)]
			if !ok {
				e = &modelLoad{Slot: slot, Model: model, StartedMS: now}
				b.entries[loadKey(slot, model)] = e
			}
			if e.Phase != loadFailed {
				was := e.Phase
				e.Phase = loadLoading
				if was != loadLoading {
					changed = append(changed, *e)
					b.fire(*e)
				}
			}
		case "unloaded":
			if has {
				delete(b.loaded[slot], model)
			}
			if e, ok := b.entries[loadKey(slot, model)]; ok && e.Phase != loadFailed && e.Phase != loadReady {
				e.Phase = loadFailed
				msg := "the router unloaded the model before it was ready"
				e.Error, e.FinishedMS = &msg, &now
				changed = append(changed, *e)
			}
		}
	}
	for _, e := range b.entries {
		if e.Slot != slot || e.Phase == loadReady || e.Phase == loadFailed {
			continue
		}
		if _, listed := models[e.Model]; !listed {
			e.Phase = loadFailed
			msg := "the router no longer lists the model"
			e.Error, e.FinishedMS = &msg, &now
			changed = append(changed, *e)
		}
	}
	for model := range b.loaded[slot] {
		if _, listed := models[model]; !listed {
			delete(b.loaded[slot], model)
		}
	}
}

func (b *loadBoard) fromLog(slot string) ([]string, *float64, *string) {
	if b.logDir == "" {
		return nil, nil, nil
	}
	b.mu.Lock()
	dir := b.logDir
	b.mu.Unlock()
	tail, err := tailFile(filepath.Join(dir, slot+".err"), 24<<10)
	if err != nil {
		return nil, nil, nil
	}
	var lines []string
	var progress *float64
	var failure *string
	for _, raw := range strings.Split(tail, "\n") {
		if !hasLoadMark(raw) {
			continue
		}
		line := strings.TrimSpace(raw)
		lines = append(lines, line)
		if p := percentIn(line); p >= 0 {
			v := p
			progress = &v
		}
		if isLoadFailure(line) {
			msg := line
			failure = &msg
		}
	}
	return lines, progress, failure
}

func lineFor(lines []string, model string) string {
	if len(lines) == 0 {
		return ""
	}
	for i := len(lines) - 1; i >= 0; i-- {
		if isLoadFailure(lines[i]) && namesModel(lines[i], model) {
			return lines[i]
		}
	}
	for i := len(lines) - 1; i >= 0; i-- {
		if model != "" && strings.Contains(lines[i], model) {
			return lines[i]
		}
	}
	return lines[len(lines)-1]
}

func namesModel(line, model string) bool {
	return model == "" || strings.Contains(line, model)
}

func hasLoadMark(line string) bool {
	for _, mark := range []string{
		"load_tensors", "llama_model_load", "llama_model_loader", "loading model",
		"load: ", "instance name=", "failed to allocate", "unable to allocate",
		"error loading model", "exited with status", "request cancelled while waiting",
	} {
		if strings.Contains(line, mark) {
			return true
		}
	}
	return false
}

func isLoadFailure(line string) bool {
	for _, mark := range []string{
		"failed to allocate", "unable to allocate", "error loading model",
		"exited with status", "out of memory", "OOM",
	} {
		if strings.Contains(line, mark) {
			return true
		}
	}
	return false
}

func percentIn(line string) float64 {
	i := strings.IndexByte(line, '%')
	if i <= 0 {
		return -1
	}
	j := i - 1
	for j >= 0 && (line[j] >= '0' && line[j] <= '9' || line[j] == '.') {
		j--
	}
	if j == i-1 {
		return -1
	}
	v, err := strconv.ParseFloat(line[j+1:i], 64)
	if err != nil || v < 0 || v > 100 {
		return -1
	}
	return v / 100
}

func tailFile(path string, n int64) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return "", err
	}
	size := st.Size()
	if size > n {
		if _, err := f.Seek(size-n, 0); err != nil {
			return "", err
		}
	}
	n = min(n, size)
	buf := make([]byte, n)
	read, err := io.ReadFull(f, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return "", err
	}
	return string(buf[:read]), nil
}

func (b *loadBoard) prune() {
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	for key, e := range b.entries {
		age := now.Sub(time.UnixMilli(e.StartedMS))
		switch e.Phase {
		case loadReady:
			if e.FinishedMS != nil && now.Sub(time.UnixMilli(*e.FinishedMS)) > loadReadyFor {
				delete(b.entries, key)
			}
		case loadFailed:
			if e.FinishedMS != nil && now.Sub(time.UnixMilli(*e.FinishedMS)) > loadFailedFor {
				delete(b.entries, key)
			}
		case loadQueued, loadLoading:
			if age > loadQueuedFor {
				delete(b.entries, key)
			}
		}
	}
}

func (b *loadBoard) forget(slot string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for key, e := range b.entries {
		if e.Slot == slot {
			delete(b.entries, key)
		}
	}
	delete(b.loaded, slot)
}

func (b *loadBoard) refreshLogs(slots []string) {
	for _, slot := range slots {
		lines, progress, failure := b.fromLog(slot)
		if len(lines) == 0 {
			continue
		}
		var changed []modelLoad
		b.mu.Lock()
		for _, e := range b.entries {
			if e.Slot != slot || e.Phase == loadReady {
				continue
			}
			if pick := lineFor(lines, e.Model); pick != "" {
				e.Line = pick
			}
			if progress != nil {
				e.Progress = progress
			}
			if failure != nil && e.Phase == loadLoading && namesModel(*failure, e.Model) {
				e.Phase = loadFailed
				e.Error = failure
				e.Line = *failure
				now := time.Now().UnixMilli()
				e.FinishedMS = &now
				changed = append(changed, *e)
			}
		}
		b.mu.Unlock()
		for _, l := range changed {
			b.fire(l)
		}
	}
}

func (b *loadBoard) snapshot() []modelLoad {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]modelLoad, 0, len(b.entries))
	for _, e := range b.entries {
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Slot != out[j].Slot {
			return out[i].Slot < out[j].Slot
		}
		return out[i].StartedMS < out[j].StartedMS
	})
	return out
}

// observeFromLog keeps the load state honest while the router cannot be asked: the last
// `load:   ./model.gguf` line names what is being loaded, and a failure after it is its own.
func (b *loadBoard) observeFromLog(slot string) {
	lines, _, _ := b.fromLog(slot)
	model := ""
	at := -1
	for i, l := range lines {
		if m := modelFromLoadLine(l); m != "" {
			model, at = m, i
		}
	}
	if model == "" {
		return
	}
	now := time.Now().UnixMilli()
	var changed *modelLoad
	b.mu.Lock()
	e, ok := b.entries[loadKey(slot, model)]
	if !ok {
		e = &modelLoad{Slot: slot, Model: model, Phase: loadLoading, StartedMS: now}
		b.entries[loadKey(slot, model)] = e
		changed = e
	}
	for _, l := range lines[at+1:] {
		if isLoadFailure(l) && namesModel(l, model) {
			if e.Phase != loadFailed {
				e.Phase = loadFailed
				msg := l
				e.Error, e.FinishedMS = &msg, &now
				changed = e
			}
			break
		}
	}
	if changed != nil {
		out := *changed
		b.mu.Unlock()
		b.fire(out)
		return
	}
	b.mu.Unlock()
}

func modelFromLoadLine(line string) string {
	i := strings.Index(line, "load:")
	if i < 0 {
		return ""
	}
	rest := line[i+len("load:"):]
	for _, field := range strings.Fields(rest) {
		if !strings.HasSuffix(field, ".gguf") {
			continue
		}
		base := filepath.Base(field)
		return strings.TrimSuffix(base, ".gguf")
	}
	return ""
}

func (d *demesne) loadChanged(l modelLoad) {
	d.hub.publish(eventFrame{Type: "load", AtMS: time.Now().UnixMilli(), Load: &l})
	d.history.record(transitionRecord{AtMS: time.Now().UnixMilli(), Slot: l.Slot, Kind: "load", To: l.Phase, Reason: l.Model})
}

// reloadPreset asks a router to re-read its models.ini. llama.cpp exposes no POST for this: the
// refresh is a query string on the model list, and it is slow while a model is loading.
func (d *demesne) reloadPreset(s *slot) (int, error) {
	if !isRouterSlot(s.specOf().bin) {
		return 0, errors.New(s.specOf().name + " is not a llama.cpp router, so it has no preset to reload")
	}
	req, err := http.NewRequest(http.MethodGet, strings.TrimRight(s.specOf().url, "/")+"/models?reload=1", nil)
	if err != nil {
		return 0, err
	}
	resp, err := (&http.Client{Timeout: 90 * time.Second}).Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("the router answered %d: %s", resp.StatusCode, engineMessage(body))
	}
	var doc struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	_ = json.Unmarshal(body, &doc)
	d.refreshModels()
	return len(doc.Data), nil
}

func isRouterSlot(bin string) bool {
	return filepath.Base(bin) == "llama-server"
}

func (d *demesne) routerModels(s *slot) (map[string]string, error) {
	req, err := http.NewRequest(http.MethodGet, strings.TrimRight(s.specOf().url, "/")+"/v1/models", nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 1500 * time.Millisecond}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errNotRouter
	}
	var doc struct {
		Data []struct {
			ID     string `json:"id"`
			Status struct {
				Value string `json:"value"`
			} `json:"status"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil, err
	}
	if len(doc.Data) == 0 {
		return nil, errNotRouter
	}
	out := make(map[string]string, len(doc.Data))
	for _, m := range doc.Data {
		if m.ID == "" {
			continue
		}
		out[m.ID] = m.Status.Value
	}
	return out, nil
}

func (d *demesne) pollLoads() {
	var active []string
	for _, s := range d.slots {
		if !s.specOf().router {
			continue
		}
		if !portAlive(s.specOf().url) {
			d.loads.forget(s.specOf().name)
			continue
		}
		models, err := d.routerModels(s)
		if err != nil {
			// /v1/models is blocked for tens of seconds while the router loads a model, so
			// the engine's own log is the only witness during exactly the window a card
			// cares about.
			d.loads.observeFromLog(s.specOf().name)
			active = append(active, s.specOf().name)
			continue
		}
		d.loads.observe(s.specOf().name, models)
		active = append(active, s.specOf().name)
	}
	d.loads.refreshLogs(active)
	d.loads.prune()
}

func (d *demesne) followLoads() {
	d.pollLoads()
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-d.done:
			return
		case <-t.C:
			d.pollLoads()
		}
	}
}
