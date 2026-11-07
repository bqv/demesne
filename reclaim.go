// reclaim.go -- taking a port back from a process that is provably ours.
//
// The estate is supposed to be the only thing that ever holds an engine's port, and since
// 2026-11-07 every child dies with demesne, so an orphan should no longer be possible. But
// "should no longer" is not "cannot": an engine started by an older demesne -- one running
// before Pdeathsig existed -- sat on port 55557 for a day and nineteen hours, and every new
// llamb crashed against it with a bare `exit status 1`.
//
// So a slot whose port is answered gets one chance to take it back, and the bar for killing
// anything is evidence, not a guess:
//
//  1. the process must be in the slot's OWN cgroup leaf. Only demesne ever moves a process
//     there, so this is close to proof of ownership; and
//  2. its executable must be the binary the slot is configured to run.
//
// A slot with no fence configured has no (1), so it is refused rather than guessed at. The
// cost of refusing is a stuck slot with a clear reason; the cost of guessing is killing
// somebody else's process, which is not a trade this program makes.

package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// procPortPID finds the pid listening on a TCP port, by the same route ss takes: the socket
// inode from /proc/net/tcp, then the process holding it in /proc/<pid>/fd.
func procPortPID(port int) (int, bool) {
	inodes := listenInodes(port)
	if len(inodes) == 0 {
		return 0, false
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, false
	}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		fds, err := os.ReadDir("/proc/" + e.Name() + "/fd")
		if err != nil {
			continue
		}
		for _, fd := range fds {
			target, err := os.Readlink("/proc/" + e.Name() + "/fd/" + fd.Name())
			if err != nil || !strings.HasPrefix(target, "socket:[") {
				continue
			}
			if inodes[strings.TrimSuffix(strings.TrimPrefix(target, "socket:["), "]")] {
				return pid, true
			}
		}
	}
	return 0, false
}

func listenInodes(port int) map[string]bool {
	want := fmt.Sprintf("%04X", port)
	out := map[string]bool{}
	for _, file := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		b, err := os.ReadFile(file)
		if err != nil {
			continue
		}
		lines := strings.Split(string(b), "\n")
		for _, line := range lines[1:] {
			fields := strings.Fields(line)
			if len(fields) < 10 || fields[3] != "0A" {
				continue
			}
			local := fields[1]
			colon := strings.LastIndexByte(local, ':')
			if colon < 0 || !strings.EqualFold(local[colon+1:], want) {
				continue
			}
			out[fields[9]] = true
		}
	}
	return out
}

// reclaimSlot takes the slot's port back from a process that is provably ours. It reports
// whether it killed anything; an error is a refusal with the reason, and the port is left
// alone.
func (d *demesne) reclaimSlot(s *slot) (bool, error) {
	pid, ok := procPortPID(s.port())
	if !ok {
		return false, nil
	}
	m := s.specOf().cgroup
	if !m.set() {
		return false, fmt.Errorf("port %d is held by pid %d, and this slot has no cgroup fence "+
			"to prove the holder is ours; refusing to kill it", s.port(), pid)
	}
	if joined, err := inCgroup(pid, m.leaf); err != nil || !joined {
		return false, fmt.Errorf("port %d is held by pid %d, which is not in %s; refusing to kill it",
			s.port(), pid, m.leaf)
	}
	exe, err := os.Readlink("/proc/" + strconv.Itoa(pid) + "/exe")
	if err != nil {
		return false, fmt.Errorf("port %d is held by pid %d, whose executable cannot be read: %v",
			s.port(), pid, err)
	}
	if filepath.Base(exe) != filepath.Base(s.specOf().bin) {
		return false, fmt.Errorf("port %d is held by pid %d, which is running %s, not %s; refusing to kill it",
			s.port(), pid, filepath.Base(exe), filepath.Base(s.specOf().bin))
	}

	log.Printf("%s: pid %d is holding port %d and is provably ours (in %s, running %s); reclaiming it",
		s.specOf().name, pid, s.port(), m.leaf, filepath.Base(exe))
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	_ = syscall.Kill(pid, syscall.SIGTERM)
	if waitPortFree(s.port(), 5*time.Second) {
		return true, nil
	}
	_ = syscall.Kill(-pid, syscall.SIGKILL)
	_ = syscall.Kill(pid, syscall.SIGKILL)
	if waitPortFree(s.port(), 5*time.Second) {
		return true, nil
	}
	return false, errors.New("port " + strconv.Itoa(s.port()) + " is still held after SIGKILL")
}

// waitPortFree waits for the listener to go, without a polling sleep process and without
// holding anything: a short ticker, and the caller's own bound.
func waitPortFree(port int, limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if !portAlive("127.0.0.1:" + strconv.Itoa(port)) {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return !portAlive("127.0.0.1:" + strconv.Itoa(port))
}
