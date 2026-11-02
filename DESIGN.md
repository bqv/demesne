# demesne — the design

This is the drill-down: the forks in the design that were still open when the fork was
cut, each one now decided, with the reason attached. It is a record of decisions, not a
plan of work — the work is in `main.go`, `slot.go` and `watch.go`, and the tests in
`slot_test.go` pin the parts that break.

Fork point: `stadi` at `ad49321`. Remit unchanged: demesne is the single owner of the
local-inference estate — the 20 GiB card, and every port an engine wants.

---

## 1. Process model — demesne fork/execs every engine it owns

**Decided:** demesne is the supervisor. It `fork/exec`s llama, llamb, sd and whisper
itself and never asks `rc-service` for anything, except for an engine that is explicitly
left to OpenRC (`policyExternal`).

Why: the scarce resource is *the card*, and the decisions about it are not
service-lifecycle-shaped. "Unload the router's models, wait for the VRAM to return, then
spawn the media child" cannot be written as an OpenRC script, and the 2026-11-02 double
restart — `supervise-daemon` respawning llama every 5 s while `recoverLLM()` asked
`rc-service` for the same restart — was an artefact of two owners, not of a bug. One
owner means one actor per restart, by construction.

What that costs, and is paid for here:

| giving up | how it is covered |
| :-- | :-- |
| `supervise-daemon`'s respawn policy | `supervise()`: restart on exit, 5 s doubling to a 60 s cap, reset after a minute of health |
| cgroup pinning in `init.d/*` (`start_post`) | per-slot `memLimit`: delegate, write the limits, join each child (`slot.go`) |
| `rc-service --user llama restart` | `POST /slots/llama/load` / `stop`, or `demesne load llama` |
| pidfiles, `rc-status` | `/status` (`demesne status`) |

Per-engine shutdown is still by SIGTERM, and demesne takes every child down with it — the
children are in their own process groups (`Setpgid`), so without that handler a service
restart orphans ~12.5 GiB that nothing owns.

---

## 2. Slot model — one generic table, four rows

**Decided:** a generic slot table. Not "one GPU slot plus one media port": that shape
cannot express llamb (a second, smaller card) or whisper (CPU-only), and it re-invents
itself per engine anyway.

The slot spec is the struct in `slot.go`:

```
name, url            identity and the port the child must bind
bin, args, env       the child spec: what to run, with what, with which environment
group, vram          the scarce resource: exclusivity group, and whether it is on the card
policy               external | on-demand | resident
idle                 evict an on-demand child after this long with no request
routed, fallback     may demesne route to it; and may it proxy to an engine it does not own
cgroup               memory.high / memory.max / memory.swap.high in a delegated leaf
```

The estate, as configured:

| slot | port | group | vram | policy | routed | why |
| :-- | --: | :-- | :-- | :-- | :-- | :-- |
| `llama` | 55555 | `gpu` | yes | resident (or external) | yes | the router; keeps a resident rung |
| `sd` | 55557 | `gpu` | yes | on demand | yes | cannot unload; must be evicted |
| `whisper` | 55559 | — | no | on demand | yes | CPU-only: contends for neither card nor group |
| `llamb` | 55556 | `aux` | yes | resident (or external) | no | second card; its clients address it directly |

Two invariants are enforced at startup, not discovered at runtime: **one slot per port**,
and **one resident holder of a card per group**.

Two policies: *resident* (spawned at boot, restarted forever, never evicted) and *on
demand* (spawned by a request, evicted when idle or when a group peer wants the card). An
engine with no command line is *external*: demesne routes to it and never touches it —
which is the whole migration story in one switch.

---

## 3. sd vs whisper — a port collision, not an exclusivity question

**Decided:** they are not exclusive. whisper is CPU-only; it never competes for the
card. What they competed for was the number 55557, and the answer to a port collision is
to stop sharing the port, not to arbitrate it at runtime.

- whisper moves to **55559**, permanently, and gets a route (`/inference`, `/v1/audio/*`)
  instead of falling through to the llama router (§6.2 of the handoff: before this,
  `/inference` matched no route and was answered by the wrong engine).
- whisper joins the slot table with `group = ""`, `vram = false`, policy on demand. It is
  evicted on its own idle clock (10 m) and never by another slot's request.
- the 55557 collision now cannot recur: two slots claiming one port is a startup error,
  and a port answered by a process demesne does not own is a refusal with a named reason,
  reported as `port_held_elsewhere` in `/status`.

Cheapest concrete win, and it needs neither the card nor a cold load: it is one line in
`conf.d/whisper` (below) plus two variables in `conf.d/demesne`.

---

## 4. cgroups — the fence moves into demesne, per slot

**Decided:** move. `rc_cgroup_settings` never worked for a `--user` service
(`openrc-run.sh:60` does not source `rc-cgroup.sh` under `RC_USER_SERVICES`, and
`_setup_cgroup` returns at `[ -w /sys/fs/cgroup ]`), which is why each init script
carried forty lines of `start_post()`; those forty lines cannot follow a child into
demesne. So demesne does the same three steps itself:

1. `sudo -n cgroup-prepare /sys/fs/cgroup/demesne <slot>` — delegate the leaf;
2. write `memory.high`, `memory.max`, `memory.swap.high` from the environment;
3. `sudo -n cgroup-join <child pid> <leaf>` after every spawn (there is no supervisor
   daemon to inherit from any more).

Warn-only, exactly as the init scripts were: a failure must never stop the service, or
the box loses an engine to a permissions problem. Limits live in `conf.d/demesne` as
`DEMESNE_<SLOT>_MEMORY_HIGH` / `_MEMORY_MAX` / `_SWAP_HIGH`; the router keeps its 5G/1G/6G,
sd keeps its 6G (its text encoder runs on the CPU and lives in host RAM), llamb its 3G/4G.

---

## 5. Restart policy — resident is eager, media is lazy, both are bounded

**Decided:** the policy is a property of the slot, and the two answers are different
because the costs are different.

- **resident** (llama, llamb): eager. Spawn at boot, restart on exit, 5 s doubling to a
  60 s cap, forever, reset after a minute of health. Never evicted. `recoverLLM()` no
  longer restarts it — it *kills* a wedged child and lets the supervisor start it, so
  there is one actor.
- **on demand** (sd, whisper): lazy, with the crash backoff the sd child never had. It
  was logged, marked down, and the next request retried immediately — each attempt able
  to pay a 15-minute cold load. Now a slot that keeps dying is spaced out (5 s doubling to
  60 s) and the request is refused with the reason and the wait.
- `waitReady()` watches the child while it waits for the port, so a child that dies during
  startup is reported in 500 ms rather than after the full 15-minute load bound.

---

## 6. Operator interface — `/status` is the only status surface

**Decided:** `/status` (JSON) is the one place to look, and the CLI is its client. The
OpenRC verbs do not disappear, they change subject: `rc-service --user demesne
start|stop|restart` is the whole estate, and per-engine control is demesne's.

```
demesne status                  # the estate, from /status
demesne release [slot]          # evict one on-demand engine (default: all that are up)
demesne load <slot>             # start it and wait for its port (cold sd ≈ 8 min)
demesne stop <slot>
```

`/status` keeps stadi's field names (`slot`, `sd_child_up`, `sd_child_pid`, `llm_health`,
`media_in_flight`, `idle_timeout_s`) and adds `slots[]` with, per engine: policy, owned,
up, booting, pid, uptime, in-flight, restarts, last error, cgroup, and
`port_held_elsewhere` when something else owns the port. A status poll never takes the
transition lock, so a cold load cannot wedge the surface that explains the cold load.

The HTTP side: `GET /status`, `GET|POST /release[?slot=]`, `GET /slots/<name>`,
`POST /slots/<name>/load|stop`.

---

## 7. What stays an OpenRC service

Everything that does not hold the card or a contested port: `dsh`, `ward`, `gpg-agent`,
`openmessage`, `rcsagent`, `adb` — and `demesne` itself, which is how it starts. The four
engines leave OpenRC as they are adopted (§8). `init.d/stadi` stays installed but unused
during the transition.

---

## 8. Migration order — whisper first, then sd, then llama, then llamb

**Decided: whisper first.** The handoff's instinct was "sd first, because demesne already
does exactly that", and that is right about *risk* but wrong about *sequence*: the port
collision can be dissolved without migrating anything, and the cheapest change should not
be postponed behind the expensive one.

| step | change | risk | rollback |
| :-- | :-- | :-- | :-- |
| 0 | `conf.d/whisper`: `WHISPER_PORT=55559`; no runlevel, service stopped | none — it is already stopped | put the number back |
| 1 | deploy `demesne` on `:55558` with the same `sd` settings (`STADI_*` still read) | low: the sd path is stadi's, unchanged in shape | `rc-service --user demesne stop`, `stadi start` |
| 2 | `DEMESNE_WHISPER_BIN/ARGS` in `conf.d/demesne`; whisper leaves the OpenRC tree | low: CPU-only, lazy, no card | unset the two variables; `whisper start` |
| 3 | `DEMESNE_LLAMA_BIN/ARGS/ENV` in `conf.d/demesne`; `rc-update del llama`; stop it | medium: the router is the busy engine | unset the variables (it becomes external, still routed), `llama start` |
| 4 | `DEMESNE_LLAMB_*`; `rc-update del llamb` | low: the aux card, small models | as above |

Step 1 is the only step that must be atomic: `stadi` and `demesne` both want `:55558`,
so one replaces the other rather than running beside it. Rollback for step 1 is: stop
demesne (it takes its children down), start stadi (which spawns sd on demand exactly as
before), start the engines that were still OpenRC's.

**Version line:** `0.3.0`. `0.2.0` is stadi's; the fork line starts here so a log line
names the right program.

---

## 9. Configuration — one `conf.d/demesne` for the estate

**Decided:** one file. The per-engine `conf.d/*` were only ever a transport for
`rc-service`'s environment; the environment each *child* needs is now a slot property
(`-<slot>-env`), which is also the only way to give the two routers different Vulkan pins
(`VK_DRIVER_FILES`) from one parent process.

Flags still win over environment, and `DEMESNE_*` wins over `STADI_*` (read, with a note
on stderr). The full set is `demesne -h`; `README.md` has the table.

```sh
# ~/.config/rc/conf.d/demesne
export DEMESNE_LISTEN="127.0.0.1:55558"
export DEMESNE_IDLE="30m"
export DEMESNE_SD_BIN="/usr/bin/sd-server"
export DEMESNE_SD_ARGS="--diffusion-model ... --listen-port 55557"
export DEMESNE_SD_ENV="VK_DRIVER_FILES=/usr/share/vulkan/icd.d/radeon_icd.x86_64.json"
export DEMESNE_SD_MEMORY_HIGH="6G"
export DEMESNE_SD_MEMORY_MAX="7G"

# step 2: whisper, on its own port and its own (CPU) group
export DEMESNE_WHISPER_BIN="/usr/bin/whisper-server"
export DEMESNE_WHISPER_ARGS="--model ... --host 127.0.0.1 --port 55559 --inference-path /inference"
export DEMESNE_WHISPER_IDLE="10m"

# step 3: adopt the router (leave these unset to leave it to OpenRC)
export DEMESNE_LLAMA_BIN="/usr/bin/llama-server"
export DEMESNE_LLAMA_ARGS="--models-dir ... --models-preset ... --host 127.0.0.1 --port 55555"
export DEMESNE_LLAMA_ENV="VK_DRIVER_FILES=/usr/share/vulkan/icd.d/radeon_icd.x86_64.json"
export DEMESNE_LLAMA_MEMORY_HIGH="5G"
export DEMESNE_LLAMA_SWAP_HIGH="1G"
export DEMESNE_LLAMA_MEMORY_MAX="6G"
```

`DEMESNE_*_ARGS` and `_ENV` are space-separated lists, like stadi's `-sd-args`: the
environment is the natural transport for a list, and a unit file quoting one through
`command_args` would need nested quotes OpenRC does not interpret.

---

## 10. The six problems in the fork snapshot

| # | problem | disposition |
| --: | :-- | :-- |
| 1 | 55557 collision, nothing enforcing exclusivity | whisper moves to 55559; duplicate ports are a startup error; a foreign holder is refused and named |
| 2 | whisper unroutable: `/inference` fell through to llama | whisper is a routed slot; unconfigured, it answers 503 naming itself |
| 3 | double restart: `recoverLLM` + `supervise-daemon` on one event | one actor — the supervisor restarts, the watch only kills a wedged child; the rc-service path survives only for an external engine |
| 4 | the sd child had no backoff | crash backoff on every on-demand slot, and `waitReady` notices a startup death in 500 ms |
| 5 | no syslog; unrotated logs | not solved, only narrowed: each slot's stdout/stderr is `$DEMESNE_LOG_DIR/<slot>.{log,err}`, one pair per engine, still unrotated (`llamb.err` was ~140 MB) |
| 6 | orphaned VRAM freezes the LLM side for 24 h | unchanged on purpose: a restart cannot free an orphaned context, and a loop would only hide the fault. `/status` now shows the three reasons separately (`orphaned`, `degraded`, `last_error`) |

---

## 11. What is still open

- **log rotation** (problem 5) is real and unaddressed; the answer is probably a `logrotate`
  config outside this repo, not code.
- **sd's `--auto-fit` + VRAM wait** interact in a way that is only understood empirically:
  the current code warns and continues when the card is not free in time, which is right
  for sd and would be wrong for an engine that OOMs cleanly.
- **llamb is supervised but not routed.** Its clients reach 55556 directly. If a route is
  ever wanted, it is a `slotSpec` literal and a path prefix — deliberately not a second
  code path.
