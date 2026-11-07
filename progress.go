package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// progress.go -- a load's percentage, measured rather than guessed.
//
// llama.cpp prints no percentage while a model loads (grepped: 747 "load_model" lines, zero
// '%'), so the log cannot answer. What it can answer is how many bytes the model child has
// READ, because demesne sets --load-mode dio: the child streams the gguf from disk instead of
// mmapping it, and /proc/<pid>/io counts exactly that. The denominator is the model file plus
// its mmproj, both named in the child's own command line, so nothing here is a time curve.
//
// The counter is cumulative for the child, so a load that reuses an already-running child
// starts from a baseline taken when the load was first observed. That baseline is why progress
// can begin a little above zero (up to one poll late) and why it is honest about its source:
// progress_source says which counter produced it.

const progressSourceIO = "io_read_bytes"

// procIO reads one child's read_bytes. read_bytes counts bytes the kernel fetched from the
// block layer -- with dio that is the gguf, not a page-cache hit.
func procIO(procRoot string, pid int) (uint64, bool) {
	b, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "io"))
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(line, "read_bytes:") {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, "read_bytes:")), 10, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	}
	return 0, false
}

// childPIDs lists the direct children of a pid, from stat rather than from ps.
func childPIDs(procRoot string, parent int) []int {
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return nil
	}
	var out []int
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile(filepath.Join(procRoot, e.Name(), "stat"))
		if err != nil {
			continue
		}
		line := string(b)
		close := strings.LastIndexByte(line, ')')
		if close < 0 {
			continue
		}
		fields := strings.Fields(line[close+1:])
		if len(fields) < 2 {
			continue
		}
		if ppid, err := strconv.Atoi(fields[1]); err == nil && ppid == parent {
			out = append(out, pid)
		}
	}
	return out
}

// childForModel finds the engine child serving one model: llama.cpp passes the model id as
// --alias to each child it spawns, so the child that carries it is the one loading it.
func childForModel(procRoot string, router int, model string) (int, string, []string) {
	for _, pid := range childPIDs(procRoot, router) {
		raw, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "cmdline"))
		if err != nil {
			continue
		}
		args := strings.Split(strings.TrimRight(string(raw), "\x00"), "\x00")
		alias, modelPath := "", ""
		for i, a := range args {
			switch a {
			case "--alias":
				if i+1 < len(args) {
					alias = args[i+1]
				}
			case "--model", "-m":
				if i+1 < len(args) {
					modelPath = args[i+1]
				}
			}
		}
		if alias != model {
			continue
		}
		cwd := ""
		if link, err := os.Readlink(filepath.Join(procRoot, strconv.Itoa(pid), "cwd")); err == nil {
			cwd = link
		}
		if modelPath != "" && !filepath.IsAbs(modelPath) && cwd != "" {
			modelPath = filepath.Join(cwd, modelPath)
		}
		extra := []string{}
		for i, a := range args {
			if a == "--mmproj" && i+1 < len(args) {
				p := args[i+1]
				if !filepath.IsAbs(p) && cwd != "" {
					p = filepath.Join(cwd, p)
				}
				extra = append(extra, p)
			}
		}
		return pid, modelPath, extra
	}
	return 0, "", nil
}

func fileSize(path string) (uint64, bool) {
	st, err := os.Stat(path)
	if err != nil || !st.Mode().IsRegular() {
		return 0, false
	}
	return uint64(st.Size()), true
}

// ratio turns bytes read since the baseline into 0..1 of the files the load must read.
func ratio(read, base, total uint64) float64 {
	if total == 0 {
		return 0
	}
	if read <= base {
		return 0
	}
	v := float64(read-base) / float64(total)
	if v > 1 {
		return 1
	}
	return v
}
