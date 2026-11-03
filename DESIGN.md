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
| `demesne` | 55555 | — | no | the service itself | — | the front door: the number every caller already means |
| `llama` | 55556 | `gpu` | yes | resident (or external) | yes | the router; keeps a resident rung |
| `llamb` | 55557 | `aux` | yes | resident (or external) | yes | second card; its models are routed by name too |
| `sd` | 55558 | `gpu` | yes | on demand | yes | cannot unload; must be evicted |
| `whisper` | 55559 | — | no | on demand | yes | CPU-only: contends for neither card nor group |

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

The HTTP side: `GET /status`, `GET /estate/card` (the small typed one, §6b), `GET|POST
/release[?slot=]`, `GET /slots/<name>`, `POST /slots/<name>/load|stop`, `POST /reload` (§12).

---

## 6a. The local channel — same API, second transport, no second protocol

**Decided:** demesne can serve the *same* handler on a unix socket as well as on its port
(`-socket` / `DEMESNE_SOCKET`, off by default, mode 0600). It does not get a second
protocol, and the estate does not get a private dialect that only its own tools speak.

The temptation is real — we control both ends of a ward↔demesne call, so why pay for HTTP?
Three reasons not to, and one reason to move the transport at all.

*What HTTP costs here:* nothing measurable. Chat is SSE at ~20 tokens/s and a render takes
eight minutes; header parsing and loopback TCP are noise against both. What a bespoke
protocol would cost: a second implementation in demesne, a second in the caller, and the
loss of the thing both of them already speak — the OpenAI dialect, streaming included,
which is exactly what `sd` and llama.cpp serve and what DSH consumes.

*Why the transport is still worth changing:* a port is a shared resource with the same
failure mode as 55557 — somebody else can hold it, and `bind` cannot tell you who. A socket
in `$XDG_RUNTIME_DIR` cannot be contended for by anyone who is not this user, the file mode
*is* the access control, and there is no listener on a port that any local process can dial.
For a caller we own, that is strictly better than `127.0.0.1:55558`; for DSH, which is a
third party configured with a URL, the port stays exactly where it is.

*The split, then:* port for other people, socket for ourselves. `curl --unix-socket
/run/user/1000/demesne.sock http://demesne/status` is the whole client.

**What ward should *not* do with it.** Ward's only local model route is
`http://127.0.0.1:55556` — llamb on the GT 1030, deliberately, so a judgement never
contends with the 7900 XT. Routing that traffic through demesne would put every ward
request on the `kindChat` path, which evicts the media child before proxying: a judgement
would kill a running SD render. The socket is for *control* — "who holds the card", "let it
go" — not for ward's inference.

---

## 6b. The card, as a typed answer — `/estate/card`

**Decided:** a second document, smaller than `/status` and not a slice of it, for the one
question a *client* asks: who holds the card, and since when.

```
GET /estate/card[?group=gpu]
{"group":"gpu","holders":["sd"],"since_ms":1793711314557,
 "slots":[{"name":"llama","state":"stopped"},{"name":"sd","state":"running"}]}
```

Why not a slice of `/status`: a client that reacts to the card needs two booleans and a
timestamp, and every field it parses becomes a field it depends on. `/status` stays the
operator's document and is free to grow; this one is not, and
`TestEstateCardIsTheSmallTypedAnswer` fails when it does. The request came from ward, which
had the choice between parsing four kilobytes for two booleans and being told to stop
asking — the smaller document is the cheaper honest answer, and it is the one that cannot
rot into a second `/status`.

Three shape decisions worth naming:

- **`holders` is an array**, because the invariant is "at most one holder *at rest*". During
  the handover from a render to a chat request both engines are up for the length of an
  eviction, and a single-valued field would have to pick one and misreport the other.
- **`since_ms` is the earliest readiness among the holders** — when the card became busy,
  not when the request was made — and **`0` when it is not known**, which is the honest
  answer for an engine demesne did not start: an external engine's readiness is someone
  else's event. The field is always present, so a client never has to test for its
  existence, and 0 is the one sentinel it does have to understand.
- **the group is the unit, not the device.** demesne never parses a driver's device name —
  `ROCm0` for llama, `vulkan0` for sd, `CUDA0` for the GT 1030 that llamb lives on. Naming
  one here would be a string from nowhere, and naming the wrong one would be worse than
  naming none. A caller that wants a label maps `gpu`/`aux` itself, where it knows which
  card it means.

A slot appears with one of three states: `running`, `loading` (started, not yet answering —
a cold sd load spends eight minutes there) or `stopped`. `loading` is in the vocabulary
because it is the only state in which the card is about to be taken but is not yet, and a
client that treats it as `stopped` will be evicted a minute later.

---

## 6c. One port, four engines — the model name decides

**Decided:** demesne answers on `55555`, the number every caller already means, and the
engines move up behind it (`llama` 55556, `llamb` 55557, `sd` 55558, `whisper` 55559). A
request names a model, and the name picks the engine; the path only says what kind of
request it is.

The old layout had it the other way round — the port picked the engine — which is why the
numbers could not be reorganised without breaking every caller, and why a model on the small
card was reachable only by knowing a different port. Both routers answer `/v1/models`; the
index is the union of those answers, refreshed on a 30 s ticker and built once before the
listener opens. An unknown model keeps the path's answer (the router), which is where such a
request went before any of this existed.

Two consequences worth stating, because both were mistakes waiting to happen:

- **the arbitration follows the slot the request reaches, not the path.** An aux-card model
  is in group `aux`, so asking for it never evicts a render from the `gpu` group. That is
  what makes ward's judge safe to route here at all.
- **the llama breaker is scoped to llama.** It is built from that engine's failures; applying
  it to the other card would refuse requests over somebody else's fault. (The first version
  of `/v1/models` proxied to a *booting* router and had to share the breaker's verdict —
  during a 41 s cold start it fed the watch and asked for a restart of a router that was
  simply starting. It now answers 503 itself and touches nothing.)

`/v1/models` is the estate's list, not one router's: merged, each entry keeping its own
fields, and when no router answers the caller gets a named 503 rather than an empty list,
because "no models" and "the router is down" are not the same answer.

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
- **ward's side of the local channel: the transport is written, the client is not.** Ward
  committed `5aa0299`: `wd_http_set_unix_socket(path)` sets `CURLOPT_UNIX_SOCKET_PATH` on
  the curl child before the fork, and the URL stays `http://<anything>/v1/...`. It declined
  to overload `wd_http_loopback()` with a socket case, correctly — that predicate is what
  decides whether a bearer is invented, and it is also the check that stops `net.request`
  reaching a non-loopback endpoint. If a unix request ever needs the same exemption, it gets
  its own flag rather than a lie in the loopback test.
- **nothing yet notices a dead engine on its own.** The measured case: llama was down from
  2026-11-02 17:34 to the next day's afternoon and nothing said so, because liveness was only
  ever asked at request time. `/estate/card` reports it (it probes an external engine's port),
  but the operator's `/status` still reports the *watch's* opinion of the router rather than
  the router's existence. A ticker with `up/down/booting` per slot is the fix, and it is the
  difference between `/status` being a fact and being a memory.
- **the control vocabulary is the operator's, and one typed answer now exists.**
  `/estate/card` (§6b) is the small document ward reacts to; `/status`, `/release` and
  `/slots/<name>/load|stop` remain shaped for a human. Anything further should be asked for
  by a client that needs it, not guessed at.

---

## 12. Live reload, and the fence that stays on

Two promises a running estate has to keep without a restart.

**Reload (`SIGHUP`, `POST /reload`, `demesne reload`, `rc-service --user demesne reload`).**
The process environment is fixed at exec — OpenRC sourced `conf.d/demesne` into a shell that
then exec'd the daemon, so a running demesne cannot see an edit through `os.Getenv`. It
therefore reads the file the way OpenRC did, with a shell, under a minimal environment so a
variable *deleted* from the file cannot survive in the daemon's own environment. (Without
that, "reload" would quietly mean "reload what was already true".)

What it applies: a child's binary, command line, environment, working directory, URL, idle
clock, memory fence, and whether demesne supervises it at all. What it refuses: `listen` and
`socket`, because those are the way in — a reload that moved them would drop every client
mid-stream. It refuses them *by name* and still applies the rest.

Only what changed is touched. A child whose own fields moved is replaced (an engine keeps the
argv it was started with, so a new argv is a new process): a resident one is stopped and its
supervisor starts it again from the new spec, an on-demand one stays down until the next
request, and every unchanged child keeps running. `specOf()` makes the spec a single value a
request can read while a reload swaps it, so a reload cannot tear a lookup.

**The fence.** Every slot's `memory.high`/`memory.max`/`memory.swap.high` is written by
demesne into a leaf, and the child is joined to it after **every** spawn — including respawns
— because a supervisor that restarts an engine must not quietly restart it outside its limit.
A timer re-asserts both the limits and the membership every minute, so "bounded" is a running
claim and not a startup one. `/status` publishes the fence as a fact: the leaf, the values,
whether the current child is inside it, when that was last true, and the error when it is not.

The tree, and the reason for its shape:

```
/sys/fs/cgroup/estate        controllers, no processes
  ctl                        demesne's supervisor
  llama/                     controllers, no processes
    llm                      the router's fence (5G high / 6G max, 1G swap)
    aux                      the aux router's fence (3G / 4G)
  sd                         the media child (6G / 8G: its text encoder is on the CPU)
  whisper
```

The two routers share a parent because they are the two halves of one job, and a parent that
holds no processes is the natural place for a limit that would one day cover both. demesne
itself sits in `ctl` — a cgroup with controllers enabled for children cannot hold processes,
so "demesne in the estate" means "demesne in a leaf of it".

This was **broken in a way nothing could see** until 2026-11-03: the fence key was built from
the slot name as written, so the code asked for `DEMESNE_sd_MEMORY_HIGH` and every limit in the
estate was silently unlimited from the day the slot model landed. `/status` now reports the
fence it actually applied, which is the only reason that class of bug stays fixed.
