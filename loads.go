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
	Tenant     string   `json:"tenant"`
	Model      string   `json:"model"`
	Phase      string   `json:"phase"`
	StartedMS  int64    `json:"started_ms"`
	FinishedMS *int64   `json:"finished_ms"`
	Error      *string  `json:"error"`
	Progress   *float64 `json:"progress"`
	// ProgressSrc names the counter behind Progress, so a client never has to guess whether a
	// bar is bytes read, a log percentage, or a clock.
	ProgressSrc string `json:"progress_source,omitempty"`
	ioBase      uint64
	ioBaseSet   bool
	total       uint64
	childPID    int
	Line        string `json:"line"`
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

func loadKey(tenant, model string) string { return tenant + "\x00" + model }

func (b *loadBoard) noteRequest(tenant, model string) {
	if model == "" {
		return
	}
	b.mu.Lock()
	if b.loaded[tenant][model] {
		b.mu.Unlock()
		return
	}
	if _, ok := b.entries[loadKey(tenant, model)]; ok {
		b.mu.Unlock()
		return
	}
	entry := &modelLoad{
		Tenant: tenant, Model: model, Phase: loadQueued, StartedMS: time.Now().UnixMilli(),
	}
	b.entries[loadKey(tenant, model)] = entry
	b.mu.Unlock()
	b.fire(*entry)
}

func (b *loadBoard) fire(l modelLoad) {
	if b.onChange != nil {
		b.onChange(l)
	}
}

// noteLoading starts (or rejoins) a load row for a tenant that has no way to report its
// own state: a per-model llama-server answers /v1/models only with its one model, and demesne
// is the one that asked for the spawn.
func (b *loadBoard) noteLoading(tenant, model string) {
	b.mu.Lock()
	e, ok := b.entries[loadKey(tenant, model)]
	if !ok {
		e = &modelLoad{Tenant: tenant, Model: model, StartedMS: time.Now().UnixMilli()}
		b.entries[loadKey(tenant, model)] = e
	}
	e.Phase = loadLoading
	e.Error, e.FinishedMS = nil, nil
	snap := *e
	b.mu.Unlock()
	b.fire(snap)
}

// notePhase closes a row it opened: ready when the engine answered, failed with the reason.
func (b *loadBoard) notePhase(tenant, model, phase string, cause error) {
	b.mu.Lock()
	e, ok := b.entries[loadKey(tenant, model)]
	if !ok {
		e = &modelLoad{Tenant: tenant, Model: model, StartedMS: time.Now().UnixMilli()}
		b.entries[loadKey(tenant, model)] = e
	}
	now := time.Now().UnixMilli()
	e.Phase, e.FinishedMS = phase, &now
	if cause != nil {
		msg := cause.Error()
		e.Error = &msg
	}
	snap := *e
	b.mu.Unlock()
	b.fire(snap)
}

func (b *loadBoard) observe(tenant string, models map[string]string) {
	var changed []modelLoad
	defer func() {
		for _, l := range changed {
			b.fire(l)
		}
	}()
	now := time.Now().UnixMilli()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.loaded[tenant] == nil {
		b.loaded[tenant] = map[string]bool{}
	}
	for model, value := range models {
		has := b.loaded[tenant][model]
		switch value {
		case "loaded":
			if !has {
				if e, ok := b.entries[loadKey(tenant, model)]; ok && e.Phase != loadFailed {
					e.Phase, e.FinishedMS = loadReady, &now
					changed = append(changed, *e)
				}
				b.loaded[tenant][model] = true
			}
		case "loading":
			b.loaded[tenant][model] = false
			e, ok := b.entries[loadKey(tenant, model)]
			if !ok {
				e = &modelLoad{Tenant: tenant, Model: model, StartedMS: now}
				b.entries[loadKey(tenant, model)] = e
			}
			if e.Phase != loadLoading {
				was := e.Phase
				e.Phase = loadLoading
				e.Error, e.FinishedMS = nil, nil
				e.ioBase, e.ioBaseSet, e.total, e.childPID = 0, false, 0, 0
				if e.ProgressSrc == progressSourceLog {
					e.Progress = nil
				}
				if was != loadLoading {
					changed = append(changed, *e)
					b.fire(*e)
				}
			}
		case "unloaded":
			if has {
				delete(b.loaded[tenant], model)
			}
			if e, ok := b.entries[loadKey(tenant, model)]; ok && e.Phase == loadLoading {
				e.Phase = loadFailed
				msg := "the router unloaded the model before it was ready"
				e.Error, e.FinishedMS = &msg, &now
				changed = append(changed, *e)
			}
		}
	}
	for _, e := range b.entries {
		if e.Tenant != tenant || e.Phase == loadReady || e.Phase == loadFailed {
			continue
		}
		if _, listed := models[e.Model]; !listed {
			e.Phase = loadFailed
			msg := "the router no longer lists the model"
			e.Error, e.FinishedMS = &msg, &now
			changed = append(changed, *e)
		}
	}
	for model := range b.loaded[tenant] {
		if _, listed := models[model]; !listed {
			delete(b.loaded[tenant], model)
		}
	}
}

func (b *loadBoard) fromLog(tenant string) ([]string, *float64, *string) {
	if b.logDir == "" {
		return nil, nil, nil
	}
	b.mu.Lock()
	dir := b.logDir
	b.mu.Unlock()

	// Both streams carry real marks: the child's stdout has load_tensors and the model loader's
	// lines, and the router's stderr has the 'load:' argument dump and instance exits. The
	// cancellation 500s live on stderr too, and they are NOT marks -- reading only that stream
	// (as this did) gave a line of 500s and no load marks at all.
	var lines []string
	var progress *float64
	for _, name := range []string{tenant + ".log", tenant + ".err"} {
		tail, err := tailFile(filepath.Join(dir, name), 24<<10)
		if err != nil {
			continue
		}
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
		}
	}

	var failure *string
	for _, name := range []string{tenant + ".log", tenant + ".err"} {
		tail, err := tailFile(filepath.Join(dir, name), 24<<10)
		if err != nil {
			continue
		}
		for _, raw := range strings.Split(tail, "\n") {
			if !isLoadFailure(raw) {
				continue
			}
			msg := strings.TrimSpace(raw)
			failure = &msg
		}
	}
	if failure != nil {
		lines = append(lines, *failure)
	}
	return lines, progress, failure
}

// attachProgress measures the bytes the model's own child has read, against the files that load
// must read. Only for a router that owns its children: an external engine's child is not ours to
// inspect, and no measurement means no bar rather than a made-up one.
func (d *demesne) attachProgress(s *tenant) {
	pid := 0
	s.mu.Lock()
	if s.up {
		pid = s.pid
	}
	s.mu.Unlock()
	if pid == 0 {
		return
	}
	b := d.loads
	b.mu.Lock()
	var active []*modelLoad
	for _, e := range b.entries {
		if e.Tenant == s.specOf().name && e.Phase == loadLoading {
			active = append(active, e)
		}
	}
	b.mu.Unlock()

	direct := len(s.specOf().models) > 0
	for _, e := range active {
		var child int
		var path string
		var extra []string
		if direct {
			// A per-model tenant IS the child: the tenant's pid is the process to measure.
			child, path = pid, modelPathFor(s.specOf())
		} else {
			child, path, extra = childForModel("/proc", pid, e.Model)
		}
		if child == 0 {
			continue
		}
		modelPath := path
		read, ok := procIO("/proc", child)
		if !ok {
			continue
		}
		b.mu.Lock()
		if e.childPID != child {
			e.childPID, e.ioBase, e.ioBaseSet, e.total = child, 0, false, 0
		}
		if e.total == 0 {
			if n, ok := fileSize(modelPath); ok {
				e.total = n
				for _, x := range extra {
					if m, ok := fileSize(x); ok {
						e.total += m
					}
				}
			}
		}
		if !e.ioBaseSet {
			e.ioBase, e.ioBaseSet = read, true
		}
		if e.total > 0 {
			v := ratio(read, e.ioBase, e.total)
			e.Progress = &v
			e.ProgressSrc = progressSourceIO
		}
		b.mu.Unlock()
	}
}

func (b *loadBoard) loadingRow(tenant string) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, e := range b.entries {
		if e.Tenant == tenant && e.Phase == loadLoading {
			return e.Model, true
		}
	}
	return "", false
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
		"error loading model", "exited with status",
	} {
		if strings.Contains(line, mark) {
			return true
		}
	}
	return false
}

func isLoadFailure(line string) bool {
	if i := strings.Index(line, "exited with status "); i >= 0 {
		code := strings.TrimSpace(line[i+len("exited with status "):])
		return code != "" && code[0] != '0'
	}
	for _, mark := range []string{
		"failed to allocate", "unable to allocate", "error loading model",
		"out of memory", "OOM",
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
		case loadQueued:
			// A request that never became a load is stale quickly.
			if age > loadQueuedFor {
				delete(b.entries, key)
			}
		case loadLoading:
			// Never by age: a 27B on the network mount reads for thirteen minutes, and
			// expiring it mid-load would make the row vanish exactly while it matters. It
			// is cleared when the tenant goes down, and a stuck engine is the engine's story
			// to tell, not something to paper over.
		}
	}
}

func (b *loadBoard) forget(tenant string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for key, e := range b.entries {
		if e.Tenant == tenant {
			delete(b.entries, key)
		}
	}
	delete(b.loaded, tenant)
}

func (b *loadBoard) refreshLogs(tenants []string) {
	for _, tenant := range tenants {
		lines, progress, _ := b.fromLog(tenant)
		if len(lines) == 0 {
			continue
		}
		var changed []modelLoad
		b.mu.Lock()
		for _, e := range b.entries {
			if e.Tenant != tenant || e.Phase == loadReady {
				continue
			}
			if pick := lineFor(lines, e.Model); pick != "" {
				e.Line = pick
			}
			if progress != nil {
				e.Progress = progress
				e.ProgressSrc = progressSourceLog
			}
			if failure := failureAfterStart(lines, e.Model); failure != "" && e.Phase == loadLoading {
				e.Phase = loadFailed
				msg := failure
				e.Error, e.Line = &msg, msg
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

// resident reports the models the tenant's engine last said were loaded, so a client does not
// have to cache the model list to know whether an unresponsive router is holding anything.
func (b *loadBoard) resident(tenant string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, 0, len(b.loaded[tenant]))
	for model, loaded := range b.loaded[tenant] {
		if loaded {
			out = append(out, model)
		}
	}
	sort.Strings(out)
	return out
}

func (b *loadBoard) snapshot() []modelLoad {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]modelLoad, 0, len(b.entries))
	for _, e := range b.entries {
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Tenant != out[j].Tenant {
			return out[i].Tenant < out[j].Tenant
		}
		return out[i].StartedMS < out[j].StartedMS
	})
	return out
}

// observeFromLog keeps the load state honest while the router cannot be asked: the last
// `load:   ./model.gguf` line names what is being loaded, and a failure after it is its own.
func (b *loadBoard) observeFromLog(tenant string) {
	lines, _, _ := b.fromLog(tenant)
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
	e, ok := b.entries[loadKey(tenant, model)]
	if !ok {
		e = &modelLoad{Tenant: tenant, Model: model, Phase: loadLoading, StartedMS: now}
		b.entries[loadKey(tenant, model)] = e
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

// failureAfterStart reports the last line that fails this model's CURRENT load: it must come
// after the model's most recent load line in the tail. A clean stop from a previous instance sits
// before it, and 'exited with status 0' is not a failure at all.
func failureAfterStart(lines []string, model string) string {
	start, last := -1, -1
	for i, l := range lines {
		if m := modelFromLoadLine(l); m != "" && m == model {
			start = i
		}
		if namesModel(l, model) && isLoadFailure(l) {
			last = i
		}
	}
	if last < 0 {
		return ""
	}
	// With a load line for this model, a failure before it belongs to an earlier attempt. With
	// none in the window, the failure is all the evidence there is -- and it is real evidence.
	if start >= 0 && last < start {
		return ""
	}
	return lines[last]
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
	d.history.record(transitionRecord{AtMS: time.Now().UnixMilli(), Tenant: l.Tenant, Kind: "load", To: l.Phase, Reason: l.Model})
}

// reloadPreset asks a router to re-read its models.ini. llama.cpp exposes no POST for this: the
// refresh is a query string on the model list, and it is slow while a model is loading.
func (d *demesne) reloadPreset(s *tenant) (int, error) {
	if !isRouterTenant(s.specOf().bin) {
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

func isRouterTenant(bin string) bool {
	return filepath.Base(bin) == "llama-server"
}

func (d *demesne) routerModels(s *tenant) (map[string]string, error) {
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
	for _, s := range d.tenants {
		if !s.specOf().router {
			continue
		}
		if !portAlive(s.specOf().url) {
			// A tenant that is starting has not bound its socket yet; forgetting its row here
			// is what made a cold load invisible for the first seconds.
			if _, loading := d.loads.loadingRow(s.specOf().name); !loading {
				d.loads.forget(s.specOf().name)
			}
			d.attachProgress(s)
			continue
		}
		models, err := d.routerModels(s)
		if err != nil {
			// /v1/models is blocked for tens of seconds while the router loads a model, so
			// the engine's own log is the only witness during exactly the window a card
			// cares about.
			d.loads.observeFromLog(s.specOf().name)
			d.attachProgress(s)
			active = append(active, s.specOf().name)
			continue
		}
		d.loads.observe(s.specOf().name, models)
		d.attachProgress(s)
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
