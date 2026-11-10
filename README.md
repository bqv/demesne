# demesne

> *demesne* — the part of a manor the lord keeps in his own hand. Here: the resident set.
> Pronounced /dɪˈmeɪn/, like "domain"; the `s` is a 16th-century scribal addition and has
> never been sounded. (`demense` is the common misspelling, and `make install` gives you it
> as a symlink.)

`demesne` is the single owner of this box's local-inference estate: four engines, two cards,
one port. It `fork/exec`s every engine it owns, decides who may hold a card and when, routes
each request to the engine that actually has the model, and answers for all of it on
`127.0.0.1:55555`.

It is a fork of `stadi` at `ad49321`, which was a tenant broker in front of *two* engines.
That repository, its checkout, its unit, its conf and its binary were all deleted once demesne
had taken the estate over; its commits live on in this history, and nowhere else. The tenant broker is the hard part and it is unchanged in spirit;
what grew is ownership — demesne no longer asks `rc-service` to start an engine, so there is
one owner and one actor per restart.

This file is both the manual and the design record: the decisions are written next to the
things they decide, because the reasons are only useful where someone is about to change the
behaviour. The work is in `main.go` (config, routing, HTTP), `tenant.go` (the lifecycle, the
fence), `watch.go` (the router's health policy), `models.go` (which engine has which model),
`reload.go` (applying a changed conf) and `cli.go`, and `tenant_test.go` pins the parts that
break.

## The estate

| | port | group | policy | role |
| :-- | --: | :-- | :-- | :-- |
| **demesne** | 55555 | — | the service | the front door: the number every caller already means |
| `llama` | 55556 | `gpu` | resident | llama.cpp router — 7–19 GiB rungs on the 20 GiB card |
| `llamb` | 55557 | `aux` | resident | llama.cpp router — the GT 1030: embedder, reranker, OCR |
| `sd` | 55558 | `gpu` | on demand | stable-diffusion.cpp; cannot unload, so it must be evicted |
| `whisper` | 55559 | — | on demand | whisper.cpp, CPU only — contends for neither card nor group |

All four are demesne's children. `llama`, `llamb` and `whisper` are out of the OpenRC runlevel
— two owners of one engine was the 2026-11-02 double restart — and their units and confs are
gone too, archived with their last config in `~/var/archive/old-engine-services-*.tar.gz`.
Nothing but demesne can claim an engine's port now, and nothing starts one but demesne.

Two invariants are enforced at startup, not discovered at runtime: **one tenant per port**, and
**one resident holder of a group**.

### Why a tenant table and not "one GPU tenant plus one media port"

Because that shape cannot express llamb (a second, smaller card) or whisper (CPU-only), and
it re-invents itself per engine anyway. One struct with one lifecycle, four rows of data:

```
name, url            identity and the port the child must bind
bin, args, env, dir  the child spec: what to run, with what, where, with which environment
group, vram          the scarce resource: exclusivity group, and whether it is on a card
policy               external | on-demand | resident
idle                 evict an on-demand child after this long with no request
routed, router       may demesne route to it; and does its /v1/models decide routing
cgroup               memory.high / memory.max / memory.swap.high in a delegated leaf
```

*Resident* engines are spawned at boot, restarted forever and never evicted. *On demand*
engines are spawned by a request and evicted when idle or when a group peer wants the card.
An engine with no command line is *external*: demesne routes to it and never touches it.

## Why an arbitrator at all

On a one-tenant card this is a correctness problem, not a scheduling nicety:

- The llama router keeps a resident model — 17.6–19.7 GiB of a 20 GiB card.
- A Qwen-Image DiT wants ~13 GiB, plus its VAE and a text encoder.
- **`sd-server` cannot unload.** No flag, no endpoint; only the *process exiting* frees the
  weights (measured: 12.5 GiB → 0.03 GiB).
- Neither engine knows the other exists, so whichever loads second OOMs mid-load.

So a plain always-on `sd-server` holds ~12.5 GiB until a human stops it, and a local model
cannot generate an image by itself: to call the image model it would have to unload the very
model making the call.

And the decisions are not service-lifecycle-shaped. "Unload the router's models, wait for the
VRAM to return, then spawn the media child" cannot be written as an OpenRC script, and the
2026-11-02 double restart — `supervise-daemon` respawning llama every 5 s while `recoverLLM()`
asked `rc-service` for the same restart — was an artefact of two owners, not of a bug.

## What happens to a request

| request | what happens |
| :-- | :-- |
| **chat** — `/v1/chat/completions`, `/v1/completions`, `/v1/embeddings`, `/v1/rerank`, `/completion`, `/infill`, `/models/load|unload` | the model name picks the engine; if that engine is on the `gpu` card, the media child is evicted, the VRAM is waited for, then the request is proxied |
| **translated** — `/v1/fim/completions` | Mistral's fill-in-the-middle dialect: `prompt` + `suffix` become llama.cpp's `input_prefix`/`input_suffix`, and the stream-shaped reply is re-enclosed as a `chat.completion`. It claims the card like chat, because it is generation |
| **anything else** — `/slots`, `/metrics`, `/tokenize`, `/detokenize`, whatever the next llama.cpp build adds | proxied to the router and **nothing else**: no card claim, no eviction, no breaker. A probe is not a claimant |
| **generation** — `/sdapi/v1/txt2img`, `/img2img`, `/v1/images/*`, `/inference`, `/v1/audio/*` | make room on the card, spawn the child, proxy to it, re-arm its idle clock |
| **metadata** — `/props`, `/health`, `/healthz` | proxy untouched; the router loads nothing for these |
| **observation** — other `/sdapi/*` (`/progress`, `/options`, …) | proxy only if loaded, otherwise `503 sd is not loaded` |

Plus a self-inflicted `keep_alive`: after `DEMESNE_IDLE` with no media request the child is
stopped and the card frees itself. That is the feature sd.cpp does not have.

The generation/observation split is not cosmetic: without it a monitoring loop polling
`/sdapi/v1/progress` would spawn the child and pin the card forever, defeating the idle timer.

The **chat / everything-else** split is the same lesson one layer out, and it was learned on
2026-11-04 from a report: the catch-all used to be chat, so `GET /slots` on the front door
evicted a running render before answering, and a failing probe fed the router's breaker — a
monitor could kill a render and then ask for a restart simply by looking at the estate. What
claims the card is now an enumerated list; everything else is proxied by a second, unwatched
proxy. The asymmetry is deliberate: a request that quietly loses its claim is a slow load, one
that quietly takes the card is somebody's ruined afternoon.

**State needs no flags: the child's existence *is* the state.** `GET /status` reports
`sd_child_up`, so "is sd loaded?" is process liveness rather than a boolean that can desync.

### One port, four engines — the model name decides

The engines keep their own numbers; demesne sits on the one every caller already means, so a
client does not need to know which engine has its model. The index is the union of both
routers' `/v1/models`, refreshed on a 30 s ticker and built once before the listener opens:

- a model only one router advertises goes there;
- an unknown model keeps the path's answer, which is the router;
- **the arbitration follows the tenant the request reaches**, so a model on the small card never
  evicts a render from the big one — that is what makes an aux-card client safe to route here;
- **the llama breaker is scoped to llama.** It is built from that engine's failures; applying
  it to the other card would refuse requests over somebody else's fault. (The first version of
  `/v1/models` proxied to a *booting* router and shared the breaker's verdict — during a 41 s
  cold start it fed the watch and asked for a restart of a router that was merely starting. It
  now answers 503 itself and touches nothing.)
- `/v1/models` is the estate's list, not one router's: merged, each entry keeping its own
  fields, and when no router answers the caller gets a named 503 rather than an empty list,
  because "no models" and "the router is down" are not the same answer.

A path still decides the *kind* of request (`/sdapi/*` is the media child, `/v1/audio/*` is
whisper); only the engine moves.

### A cold load is long enough to break a client

A 27B rung on this box takes minutes to load, and llama.cpp's router says nothing until it is
loaded. Two things follow, and both were measured the hard way on 2026-11-07, when a session
died of it:

- **demesne answers a streaming chat request itself, before dialling the engine**, and keeps
  the stream warm with SSE comments (`: demesne: the engine is still loading`) every ten
  seconds while it waits. Llama's answer is then relayed verbatim, flushed per chunk. The cost
  is that once `200` has gone out a later failure can no longer be a status code: it becomes
  an `event: error` in the stream. That price is paid only by requests that ask to stream,
  which are exactly the requests a cold load used to kill — and the hard refusals (engine
  booting, breaker open) still happen first, with their proper status.
- **the load belongs to the estate, not to the caller.** The client's cancellation is not
  propagated to the engine until the engine has answered; before that the request is kept
  alive deliberately and the caller is simply forgotten. One impatient client must not destroy
  a load that every other client is waiting for. After the engine has answered, cancellation is
  honoured as usual.

### The one dialect we translate

`POST /v1/fim/completions` is served, and it is the only translation in demesne — everything
else is proxied in the dialect the engine already speaks. The rule that decides it: **a dialect
is cheap when it renames and expensive when it re-vocabularises.**

Mistral's FIM endpoint is the only widely-implemented *name* for the thing llama.cpp calls
`/infill` — send a prefix and a suffix, get the text between them — and no OpenAI-shaped client
can reach `/infill` at all. So it earns three translations and nothing else:

- a field rename: `prompt`/`suffix` → `input_prefix`/`input_suffix`;
- a `model` field added, because the router picks the rung from it;
- a response enclosure: `/infill` is stream-shaped and not OAI-compatible, so the reply is
  re-wrapped into a `chat.completion`, or into OpenAI-shaped SSE chunks terminated by
  `data: [DONE]` when the caller asks to stream.

Two refusals, both deliberate. It does not invent an answer for a model that cannot infill:
llama.cpp requires the model to carry `fim_pre`/`fim_suf`/`fim_mid` tokens, and a model without
them answers `501 Infill is not supported by this model`, which is passed through with its status
and its reason because that is the true answer. And a FIM field with no upstream meaning is a
`422`, not a silent drop.

## Operator interface — `/status` is the only status surface

`/status` (JSON) is the one place to look, and the CLI is its client. The OpenRC verbs did not
disappear, they changed subject: `rc-service --user demesne start|stop|restart` is the whole
estate, and per-engine control is demesne's.

It keeps stadi's field names (`slot`, `sd_child_up`, `sd_child_pid`, `llm_health`,
`media_in_flight`, `idle_timeout_s`) and adds `tenants[]` with, per engine: policy, owned, up,
booting, pid, uptime, in-flight, restarts, last error, its cgroup, and `port_held_elsewhere`
when something else owns the port.

Two of those are there because a meter asked: `last_error` says why an engine is not running
and is **cleared when it becomes ready**, so a value is never a stale success; `last_transition`
is the same idea as a closed vocabulary — `idle`, `chat-claims-card`, `operator`, `failed` —
because "stopped" and "stopped because a chat took the card" are different sentences to a
reader, and only the second one explains a render that vanished. Absent while running, for the
same reason as the error; and **total for a tenant that is down**, so a tenant that has never been
asked says `idle` rather than leaving the client to interpret an absent field. An operator stop
of a tenant that is already down does not overwrite `failed` — the failure is the last thing that
actually happened.

A status poll never takes the transition lock, so a cold load cannot wedge the surface that
explains the cold load. State lives behind per-tenant mutexes; the transition lock is only for
transitions.

The HTTP side: `GET /status`, `GET /estate/card`, `GET|POST /release[?tenant=]`,
`GET /slots/<name>`, `POST /slots/<name>/load|stop`, `POST /reload`, plus the proxied
`/v1/*`, `/sdapi/*` and `/inference`.

There is no `restart` verb, and for a resident engine it is not needed: `stop` **is** a
restart, because the supervisor owns the bringing-back. Measured on llamb: stop at 17:40:43,
"is gone; restarting in 5s" at 17:40:52, started 17:40:57, ready 17:41:13. On an on-demand
engine `stop` is simply stop, and the next request starts it. demesne does not restart itself
over RPC on purpose — the call would lose its own response, and a wedged release must have an
actor outside the process; that is `rc-service --user demesne restart`.

## What each engine is loading, and what the estate is doing

Three additive surfaces, none of which touch `/status`'s existing keys or `/estate/card`:

| surface | for | shape |
| :-- | :-- | :-- |
| `loads[]` in `/status` | the cold start of a card app | one entry per model load, per tenant |
| `GET /estate/metrics` | a first paint and a fallback poll | a versioned snapshot, `"version": 1` |
| `GET /estate/events` | the live feed | SSE, a snapshot frame then typed deltas |

### `loads[]`

`tenants[].booting` is about a *child process*: a router that is up and loading a model into
VRAM is not booting, and for two hours on 2026-11-07 nothing in `/status` said so. `loads[]` is
the model-level answer, and it is an array because llamb can hold four models at once:

```json
{"tenant":"llama","model":"Qwen3.8-27B-abliterated-120k","phase":"loading",
 "started_ms":1794051974420,"finished_ms":null,"error":null,
 "progress":null,"line":"llama_model_loader: loaded meta data (45%)"}
```

- `phase`: `queued` (a request named it and the router has not started), `loading`, `ready`
  (lingering for 60 s), `failed` (lingering for 10 min, with `error`).
- `started_ms`/`finished_ms`: epoch ms. `started_ms` for a polled load is when demesne first
  saw it, which is within one 2 s tick of the truth.
- `line` and `progress` come from the child's own log (`<logDir>/<tenant>.err`) — the last
  `load_tensors`/`llama_model_loader`/`request cancelled while waiting` line, and a percentage
  when the engine prints one.
- The phase comes from the router's own `/v1/models` `status.value`, not from guessing at
  request counters: the router knows which model is loading, and a probe of `/v1/models` claims
  no card and evicts nothing.

### Two reloads, and they are not the same thing

`estate.reload` re-reads **demesne's** own conf (`~/etc/conf.d/demesne`), which is why a
restart is not needed for a port change, a fence change or an env change.

`POST /tenants/llama/reload` (or `tenants.reload` over RPC) asks **the router** to re-read
`~/var/model/llm/models.ini`, so adding, editing or deleting a rung needs no restart either.
llama.cpp has no POST for this: the refresh is a query string on the model list,
`GET <router>/models?reload=1`, and it is slow — ten seconds or more — while a model is loading.
demesne gives it a 90 s budget and refreshes its own model index as soon as it returns.

Two things follow from the router's shape, both learned on 2026-11-07: the reload is the
*only* live way to change a preset (the endpoint is not `POST /models/reload`, which is a 404 in
this build), and `/v1/models` can block for tens of seconds while a model loads — so the load
board falls back to the child log (the last `load:   ./model.gguf` line) exactly when a card
needs it most.

### `GET /estate/metrics`

One document, `"version": 1`, with `since_ms` naming the moment every counter is counted from
(the daemon's start). Per tenant: `runtime` (`requests_total`, `active`, `errors_total`,
`last_latency_ms`), `cgroup` (`current_bytes`, `high_bytes`, `max_bytes`, `oom_kills`), `load`,
`restarts`, `uptime_s`, `foreign`, `last_transition`. Estate-wide: VRAM used/total/free, the
models each router advertises (`models`), which tenant owns each model (`owner`), `history` (the
last 32 transitions, with the reason a tenant stopped), and an `unavailable` list — currently
`tokens_in`, `tokens_out`, `prefill_tps`, `decode_tps`, `queue_depth`, `wait_ms`, because
reading those means parsing response bodies, which this daemon deliberately does not do. They
are named rather than silently zero.

### `GET /estate/events`

Server-sent events on the same listener. The first frame is a **complete snapshot** — the
`/estate/metrics` document — so a client paints from one frame and needs no second call. After
that, deltas arrive as `transition` and `load` frames, and a coalesced `snapshot` is published
every 2 s while anyone is subscribed (the metrics tick; a lagging client is never blocked —
publishes drop instead, and the next snapshot repairs the client's view).

```
id: 41
event: load
data: {"seq":41,"type":"load","at_ms":1794051980000,"load":{...}}
```

- every frame carries a monotonic `seq` and its own `id:` line;
- `Last-Event-ID` resumes: the last 64 frames are kept, and a reconnect replays what it missed;
  if the cursor is older than the ring, the stream starts again with a snapshot;
- `: keep-alive` every 15 s;
- subscribing claims no card, evicts nothing and spawns nothing: it reads state;
- the stream closes on shutdown, so a reload is a clean end rather than a hang.

## The typed control plane — JSON-RPC on the door we already have

`POST /rpc` speaks **JSON-RPC 2.0**, on the port and on the socket alike. The methods are the
point; the framing is not.

```sh
curl --unix-socket /run/user/1000/demesne.sock http://demesne/rpc \
  -d '{"jsonrpc":"2.0","id":1,"method":"estate.card","params":{"group":"gpu"}}'
```

| method | params | answer |
| :-- | :-- | :-- |
| `estate.status` | — | `{version, tenants[]}` — the estate's own account of itself |
| `estate.card` | `{group?}` | the small card document (default `gpu`) |
| `estate.reload` | — | the reload report |
| `tenants.status` | `{tenant}` | one tenant |
| `tenants.load` | `{tenant}` | `{tenant, state:"running"}` — may block for a cold load |
| `tenants.stop` | `{tenant}` | `{tenant, state:"stopped"}` |
| `tenants.reclaim` | `{tenant}` | `{tenant, reclaimed:bool}` — see the evidence rules below |
| `tenants.reload` | `{tenant}` | `{tenant, reloaded:true, models:N}` — re-reads a router's `models.ini` |

Errors are JSON-RPC's own: `-32601` no such method, `-32602` bad params (an unknown tenant comes
with the valid names in `data`), `-32603` internal. Batches are answered in order, and a
notification — a request with no `id` — is executed and not answered.

**Why HTTP as the transport.** It is already on the socket, ward's client already speaks it
(`CURLOPT_UNIX_SOCKET_PATH`), and Go's standard library has no CBOR: a second binary framing
would mean a dependency in a stdlib-only daemon, or a hand-rolled codec, for a payload of a few
hundred bytes. What changes here is the vocabulary — a caller says `estate.card` instead of
knowing a URL, a query string, and which of four documents answers it — and that is the part
that was worth having.

## The card, in one small document

`/status` is the operator's document and it may grow. A *client* that only wants to know
whether a card is busy gets a separate, deliberately tiny one:

```sh
curl --unix-socket /run/user/1000/demesne.sock 'http://demesne/estate/card?group=gpu'
{"group":"gpu","holders":[],"since_ms":0,
 "tenants":[{"name":"llama","state":"running"},{"name":"sd","state":"stopped"}]}
```

A client that reacts to the card needs two booleans and a timestamp, and every field it parses
becomes a field it depends on. `/status` stays the operator's and is free to grow; this one is
not, and `TestEstateCardIsTheSmallTypedAnswer` fails when it does.

- **`holders` is an array**, because the invariant is "at most one holder *at rest*": during
  the handover from a render to a chat request both engines are up for the length of an
  eviction, and a single-valued field would have to pick one and misreport the other.
- **`holders` means the card, not the process.** For the media child those are the same thing;
  idle, a router holds a port and about 28 MiB, so it counts only when a request is in flight
  or when it reports a model `loaded`. A build that publishes no per-model status is treated
  as *unknown*, not empty, and falls back to the process — the coarse answer, but never the
  wrong way round.
- **`since_ms`** is the earliest readiness among the holders — when the card became busy, not
  when the request was made — and `0` when it is not known, which is the honest answer for an
  engine demesne did not start. The field is always present, so a client never has to test for
  its existence, and 0 is the one sentinel it has to understand.
- **the group is the unit, not the device.** demesne never parses a driver's device name —
  `ROCm0` for llama, `vulkan0` for sd, `CUDA0` for the GT 1030 that llamb lives on. Naming one
  here would be a string from nowhere, and naming the wrong one is worse than naming none. A
  caller that wants a label maps `gpu`/`aux` itself, where it knows which card it means.

A tenant appears with one of three states: `running`, `loading` (started, not yet answering — a
cold sd load spends eight minutes there) or `stopped`. `loading` is in the vocabulary because
it is the only state in which the card is about to be taken but is not yet, and a client that
treats it as `stopped` will be evicted a minute later.

## Two ways in

One handler, two transports:

```sh
curl http://127.0.0.1:55555/status
curl --unix-socket /run/user/1000/demesne.sock http://demesne/status
```

There is deliberately **no second protocol**. We control both ends of a ward↔demesne call, so
the temptation is real — but HTTP costs nothing measurable here (chat is SSE at ~20 tok/s and
a render takes eight minutes), while a bespoke dialect would mean a second implementation on
both sides and the loss of the OpenAI contract that llama.cpp, sd, whisper and DSH already
speak, streaming included.

What the socket buys is the absence of a port: nothing to collide with, no listener any local
process can dial, and a file mode that *is* the access control. So: **port for other people,
socket for ourselves.**

It is for **control**, not for inference. Ward's judge dials `llamb` directly (55557), because
the aux card needs no arbitration and a judgement must never be the request that evicts a
render. Ward declined to overload its `wd_http_loopback()` with a socket case, correctly: that
predicate decides whether a bearer is invented and is also the check that stops `net.request`
reaching a non-loopback endpoint. If a unix request ever needs the same exemption, it gets its
own flag rather than a lie in the loopback test.

## Use

```sh
demesne status                  # the whole estate, from /status
demesne release                 # evict every on-demand engine that is up
demesne release sd              # ... or just one
demesne load sd                 # start it and wait for the port (cold ≈ 8 min)
demesne stop sd
demesne reload                  # re-read conf.d; only what changed is touched
demesne version
```

`demesne` with no subcommand runs the estate, and OpenRC starts that:

```sh
rc-service --user demesne start|stop|restart|status
rc-service --user demesne reload     # the same as `demesne reload`, via the socket
```

Per-engine `start|stop` is demesne's now, because `rc-service` cannot see a child demesne
forked. Clients configured with a URL (DSH's local providers, for one) talk to `:55555` and
notice nothing.

### What that cost

| giving up | how it is covered |
| :-- | :-- |
| `supervise-daemon`'s respawn policy | `supervise()`: restart on exit, 5 s doubling to a 60 s cap, reset after a minute of health |
| cgroup pinning in `init.d/*` (`start_post`) | per-tenant `memLimit`: build the leaf, write the limits, join each child (`tenant.go`) |
| `rc-service --user llama restart` | `POST /tenants/llama/load` / `stop`, or `demesne load llama` |
| pidfiles, `rc-status` | `/status` (`demesne status`) |

Per-engine shutdown is still by SIGTERM, and demesne takes every child down with it — the
children are in their own process groups (`Setpgid`), so without that handler a service
restart orphans ~12.5 GiB that nothing owns.

**The handler cannot cover SIGKILL, so every child also carries `Pdeathsig: SIGKILL`.** That
half is not theory: an aux router started 1 day 19 hours earlier by a demesne that had been
SIGKILLed was still holding port 55557, and the next llamb could not bind — it reached "ready"
on the orphan's port, died with a bare `exit status 1`, and crash-looped eleven times with
nothing in any log to say why. Measured A/B: with the deadline in place the child is gone and
the port free when the daemon is killed; without it the child sits there, listening.

Two smaller consequences of the same lesson, now enforced in the spawn path:

- **a port answered by a process demesne does not own is a REFUSAL**, not a race — spawning
  onto it produces a child that cannot bind and a crash loop that reads as the engine's fault;
- **readiness is our child answering**, not just a port answering: the child is checked alive
  *before* the port is believed, so an orphan cannot be adopted as a successful start.

**And one chance to take a port back — on evidence, never on a guess.** A tenant whose port is
answered while the tenant is down tries `reclaim` (also `POST /tenants/<name>/reclaim`, and
`tenants.reclaim` over the RPC). It kills the holder only if both of these hold:

1. the process is in **the tenant's own cgroup leaf** — only demesne ever moves a process there;
2. its **executable is the binary the tenant is configured to run**, matched by basename.

Anything else is refused with the pid and the reason, and left alone: a tenant stuck behind
somebody else's listener is a loud, recoverable state, while killing the wrong process is not
recoverable at all. A tenant with no fence configured has no test 1, so it refuses too. The
holder is found the way `ss` finds it — the socket inode from `/proc/net/tcp{,6}`, then the
process in `/proc/<pid>/fd` — because a daemon may not shell out to `ss`.

### Restart policy — resident is eager, media is lazy, both bounded

The policy is a property of the tenant, and the two answers differ because the costs differ.

- **resident** (llama, llamb): eager. Spawn at boot, restart on exit, 5 s doubling to a 60 s
  cap, forever, reset after a minute of health. Never evicted. The watch no longer restarts
  them — it *kills* a wedged child and lets the supervisor start it, so there is one actor.
- **on demand** (sd, whisper): lazy, with the crash backoff the sd child never had. It used to
  be logged, marked down, and retried immediately by the next request — each attempt able to
  pay a 15-minute cold load. Now a tenant that keeps dying is spaced out and the request is
  refused with the reason and the wait.
- `waitReady()` watches the child while it waits for the port, so a child that dies during
  startup is reported in 500 ms rather than after the full 15-minute load bound.

## Reload, without restarting

The process environment is fixed at exec — OpenRC sourced `conf.d/demesne` into a shell that
then exec'd the daemon — so a running demesne cannot see an edited file through `os.Getenv`.
It reads the file instead: with a shell, under a minimal environment, so a variable *deleted*
from the file cannot survive in the daemon's own. Without that, "reload" would quietly mean
"reload what was already true".

```sh
rc-service --user demesne reload    # or: demesne reload, or POST /reload, or SIGHUP
{"restarted":["sd"],"refused":null,"updated_in_place":null,"unchanged":["llama","llamb","whisper"]}
```

- **Applies**: a child's binary, command line, environment, working directory, URL, idle
  clock, memory fence, and whether demesne supervises it at all.
- **Refuses, by name**: `listen` and `socket` — they are the way in, and moving them under a
  live instance would drop every client mid-stream. The rest of the file still applies.
- **Only what changed is touched.** A child whose own fields moved is replaced (an engine
  keeps the argv it was started with): a resident one by its supervisor, an on-demand one on
  the next request. Everything unchanged keeps running.

`specOf()` makes the spec a single consistent value a request can read while a reload swaps it,
so a reload cannot tear a lookup.

## The fence

Every tenant has a memory fence under `/sys/fs/cgroup/estate`, and the child is joined to it
after **every** spawn — including respawns, because a supervisor that restarts an engine must
not quietly restart it outside its limit. A one-minute timer re-asserts both the limits and the
membership, so "bounded" is a running claim and not a startup one. `/status` publishes the
fence as a fact: the leaf, the values, whether the current child is inside it, when that was
last true, and the error when it is not.

```
/sys/fs/cgroup/estate        controllers, no processes
  ctl                        demesne's supervisor
  llama/                     controllers, no processes
    llm                      the router      5G high / 6G max / 1G swap
    aux                      the aux router  3G / 4G
  sd                         the media child 6G / 8G (its text encoder is on the CPU)
  whisper                    whisper         1G / 2G
```

The two routers share a parent because they are the two halves of one job, and a parent that
holds no processes is the natural place for a limit that would one day cover both. demesne
itself sits in `ctl` — a cgroup with controllers enabled for children cannot hold processes, so
"demesne in the estate" means "demesne in a leaf of it".

**Only one act needs privilege: the unit putting the supervisor into `estate/ctl`.** After that
demesne owns the subtree and does everything else itself. Measured 2026-11-03 as `stem`, no
sudo: `mkdir` inside `/estate` works, the files cgroupfs creates are owned by `stem`,
`cgroup.subtree_control` can be written, and `memory.high` can be set in the grandchild — but
moving a process in from a root-owned ancestor is `EPERM`, which is exactly the one thing the
unit does. `cgroup-prepare`/`cgroup-join` remain as the fallback for a subtree owned by root (a
first start, a rebuilt `/sys/fs/cgroup`).

Warn-only, exactly as the init scripts were: a failure must never stop the service, or the box
loses an engine to a permissions problem.

Why not libcgroup: every operation it offers is a write to a file under `/sys/fs/cgroup`, which
is what demesne already does in Go. A C library would mean cgo or a dependency for
`os.WriteFile`, and it cannot make an unprivileged process privileged — the boundary was never
the interface, it was the ownership.

## Configuration

One file: `~/.config/rc/conf.d/demesne` (which is `~/etc/conf.d/demesne` here).

```sh
export DEMESNE_LISTEN="127.0.0.1:55555"
export DEMESNE_SOCKET="/run/user/1000/demesne.sock"
export DEMESNE_CONF="/home/user/etc/conf.d/demesne"     # what a reload re-reads
export DEMESNE_CGROUP_ROOT="/sys/fs/cgroup/estate"

export DEMESNE_LLAMA="http://127.0.0.1:55556"
export DEMESNE_LLAMA_BIN="/usr/bin/llama-server"
export DEMESNE_LLAMA_DIR="/home/user/var/model/llm"     # models.ini is relative to it
export DEMESNE_LLAMA_ARGS="--models-dir … --models-preset … --port 55556"
export DEMESNE_LLAMA_ENV="HIP_VISIBLE_DEVICES=0 VK_DRIVER_FILES=…"
export DEMESNE_LLAMA_MEMORY_HIGH="5G"
export DEMESNE_LLAMA_MEMORY_MAX="6G"
export DEMESNE_LLAMA_SWAP_HIGH="1G"
```

One file because the per-engine `conf.d/*` were only ever a transport for `rc-service`'s
environment; the environment each *child* needs is a tenant property (`-<tenant>-env`), which is
also the only way to give the two routers different Vulkan pins from one parent process.

**Flags first, environment as the fallback.** Every setting is a command-line argument; each
falls back to `DEMESNE_<NAME>`, then to `STADI_<NAME>` (still read, with a note on stderr),
then to a portable default. `demesne -h` is the authoritative list. One engine's keys, all of
them: `DEMESNE_<SLOT>` is the URL, then `_BIN`, `_DIR`, `_ARGS`, `_ENV`, `_MEMORY_HIGH`,
`_MEMORY_MAX`, plus `_SWAP_HIGH` (llama) and `_IDLE` (the on-demand two).

| flag | environment | default |
| :-- | :-- | :-- |
| `-listen` | `DEMESNE_LISTEN` | `127.0.0.1:55555` |
| `-socket` | `DEMESNE_SOCKET` | none — the same API on a unix socket, mode 0600 |
| `-llm` | `DEMESNE_LLAMA` (was `DEMESNE_LLM`) | `http://127.0.0.1:55556` — the router's URL |
| `-llamb` | `DEMESNE_LLAMB` | `http://127.0.0.1:55557` |
| `-sd` | `DEMESNE_SD` | `http://127.0.0.1:55558` — where the **child** comes up |
| `-whisper` | `DEMESNE_WHISPER` | `http://127.0.0.1:55559` |
| `-idle` | `DEMESNE_IDLE` | `5m` — evict an on-demand child after this long with no request |
| `-whisper-idle` | `DEMESNE_WHISPER_IDLE` | `10m` |
| `-busy-wait` | `DEMESNE_BUSY_WAIT` | `2m` — how long a chat request waits for a render, then 503 |
| `-vram-wait` | `DEMESNE_VRAM_WAIT` | `45s` — bound on waiting for the card to come back |
| `-load-wait` | `DEMESNE_LOAD_WAIT` | `15m` — bound on a child's port appearing |
| `-conf` | `DEMESNE_CONF` | none — the file `SIGHUP` / `POST /reload` re-reads |
| `-log-dir` | `DEMESNE_LOG_DIR` | `$XDG_CACHE_HOME/demesne` — one `<tenant>.{log,err}` pair per engine |
| `-restart-delay` | `DEMESNE_RESTART_DELAY` | `5s` — doubling to a `60s` cap, reset after a minute of health |
| `-restart-window` / `-restart-max` | `DEMESNE_RESTART_*` | `30m` / `2` |
| `-breaker` / `-fail-threshold` | `DEMESNE_BREAKER` / `DEMESNE_FAIL_THRESHOLD` | `20s` / `2` — the router's breaker |
| `-<tenant>-bin` | `DEMESNE_<SLOT>_BIN` | found on `PATH`; **empty leaves that engine to OpenRC** |
| `-<tenant>-args` | `DEMESNE_<SLOT>_ARGS` | **required when adopting** — the child's whole command line |
| `-<tenant>-env` | `DEMESNE_<SLOT>_ENV` | extra `KEY=VALUE` for that child, e.g. its Vulkan pin |
| `-<tenant>-dir` | `DEMESNE_<SLOT>_DIR` | the child's working directory — a router's `models.ini` is relative to it |
| `-cgroup-root` | `DEMESNE_CGROUP_ROOT` | `/sys/fs/cgroup/estate` |
| `-cgroup-prepare` / `-cgroup-join` | `DEMESNE_CGROUP_*` | on `PATH`, then `~/bin` — the *privileged fallback* only |
| `-free-gib` | — | `2` — treat the card as free below this many GiB used |
| `-llm-service` / `-llm-restart-wait` | `DEMESNE_LLM_*` | `llama` / `3m` — legacy: the restarter for an external router |
| `-version` | — | print the version and exit |

Memory fences are environment-only, three per tenant, because they are set once and there would
be twelve flags: `DEMESNE_{LLAMA,LLAMB,SD,WHISPER}_{MEMORY_HIGH,MEMORY_MAX,SWAP_HIGH}`.

Two notes on the list-valued settings. They are space-separated — the environment is the
natural transport for a list, and a unit file quoting one through `command_args` would need
nested quotes OpenRC does not interpret. And `-sd-args` must **not** include
`--params-backend`: that flag *disables* `--auto-fit`, which performs the graph segmentation
that tiles a VAE decode too large for the card. Pinning `--backend` is safe; pinning the
*params* split throws the fallback away.

## What stays an OpenRC service

Everything that does not hold a card or a contested port: `dsh`, `ward`, `gpg-agent`,
`openmessage`, `rcsagent`, `adb` — and `demesne` itself, which is how it starts.

`stadi` is gone: unit, conf, binary, checkout, repository and cgroup. There is no second
daemon to fall back to, so the rollback for a bad release is demesne's previous commit rather
than a parallel owner — which is the point of having one owner, and the reason the migration
was done one engine at a time while the old one was still installed.

## How it got here

The migration went whisper first, then sd, then the two routers. The instinct was "sd first,
because the demesne path is stadi's unchanged"; that is right about *risk* and wrong about
*sequence* — the port collision could be dissolved without migrating anything, and the cheapest
change should not wait behind the expensive one.

| step | change | risk | rollback |
| :-- | :-- | :-- | :-- |
| 0 | `conf.d/whisper` onto its own port, no runlevel | none — already stopped | put the number back |
| 1 | deploy demesne with the same `sd` settings (`STADI_*` still read) | low: the sd path is stadi's, unchanged in shape | stop demesne, start stadi (until stadi was deleted) |
| 2 | `DEMESNE_WHISPER_*`; whisper leaves the OpenRC tree | low: CPU-only, lazy, no card | unset, `whisper start` |
| 3 | `DEMESNE_LLAMA_*`; `rc-update del llama` | medium: the router is the busy engine | unset (it becomes external, still routed) |
| 4 | `DEMESNE_LLAMB_*`; `rc-update del llamb` | low: the aux card, small models | as above |

Step 1 was the only atomic one, because `stadi` and `demesne` both wanted the front port — and
that rollback existed only while stadi did. The
final layout goes one step further than the plan: demesne took **55555** — the number every
caller already meant — and the engines moved up behind it, which is what turned "the port picks
the engine" into "the model name picks the engine".

**Version line:** `0.3.0`. `0.2.0` is stadi's; the fork line starts here so a log line names
the right program.

## The six problems in the fork snapshot

| # | problem | disposition |
| --: | :-- | :-- |
| 1 | the 55557 collision, nothing enforcing exclusivity | whisper moved; duplicate ports are a startup error; a foreign holder is refused and named |
| 2 | whisper unroutable: `/inference` fell through to llama | whisper is a routed tenant; unconfigured it answers 503 naming itself |
| 3 | double restart: the watch and `supervise-daemon` on one event | one actor — the supervisor restarts, the watch only kills a wedged child |
| 4 | the sd child had no backoff | crash backoff on every on-demand tenant, and `waitReady` notices a startup death in 500 ms |
| 5 | no syslog; unrotated logs | not solved, only narrowed: one `<tenant>.{log,err}` pair per engine, still unrotated |
| 6 | orphaned VRAM freezes the LLM side for 24 h | unchanged on purpose: a restart cannot free an orphaned context, and a loop would only hide the fault. `/status` shows the three reasons separately (`orphaned`, `degraded`, `last_error`) |

### Tenants from a file

By default the five built-in tenants are what runs. `DEMESNE_TENANTS=/home/user/etc/tenants.ini`
(or `-tenants`) replaces that with a file, one section per tenant; a section named after a built-in
replaces it, and a new name adds a tenant. The built-ins stay as the defaults, so an estate that
configures nothing behaves as it did.

```ini
[spark]
url         = unix:/home/user/run/models/spark.sock
bin         = /usr/bin/llama-server
args        = -m /home/user/var/model/llm/Spark-X2.5-4B-Q4_K_M.gguf --alias spark --host /home/user/run/models/spark.sock -c 32768 -ngl 99
models      = Spark-X2.5-4B-Q4_K_M
aliases     = spark, spark-4b
kind        = model
group       = gpu
policy      = on-demand
vram_mb     = 3200
idle        = 5m
memory_high = 2G
memory_max  = 3G
```

`kind` is `model` (chosen by model name), `media` (`/sdapi/*`) or `audio` (`/v1/audio/*`). `policy`
is `on-demand` or `external`; **`warm` and `resident` are refused** — nothing in this estate is warm,
and the idle timer is the knob instead. `vram_mb` is the declared peak card footprint, `read_mb_s`
overrides the direct-read rate a reload is priced at, and `idle` falls back to `DEMESNE_IDLE`.
A tenant's cgroup leaf is `estate/<name>`; the five built-ins keep `estate/llama/{llm,aux}` and the
others exactly as they are, because moving a live cgroup moves its accounting history.

Check a file before restarting into it:

```
demesne check -tenants /home/user/etc/tenants.ini
```

It prints one line per tenant in name order — `kind`, `policy`, `group`, `url`, models, then the
three eviction inputs `vram_mb`, `reload_s` and `idle` — and refuses what would lie: an unknown key,
`kind=model` with nothing to address it, a relative or non-`.sock` unix path, a duplicate section, a
key outside a section. It notes every tenant that replaces a built-in. A malformed file is fatal at
startup; `reload` deliberately does not re-read it, because a tenant's identity, kind and cgroup
cannot change under a running child — restart to apply it.

The word **slot** is reserved for llama-server's own parallel lanes (`n_parallel`, `GET /slots`):
that path is proxied to the engine untouched. demesne's operator interface is `/tenants/<name>`.

### A tenant that listens on a socket instead of a port

Set the tenant's URL to `unix:/abs/path.sock` and the engine to bind the same path — llama.cpp
treats a `--host` ending in `.sock` as AF_UNIX and ignores the port:

```sh
export DEMESNE_STRATA="unix:/run/user/1000/models/strata.sock"
export DEMESNE_STRATA_ARGS="--host /run/user/1000/models/strata.sock ..."
```

Liveness is a **dial**, never a `stat`: `ENOENT` and `ECONNREFUSED` both mean dead, and a socket
file left behind by a `SIGKILL` is not an engine.

The directory the socket lives in is **demesne's**: `pam` creates `/run/user/<uid>` and nothing
creates its children, so before spawning demesne makes the parent directory `0700` — and refuses a
path that exists as anything but a directory it owns and that no group or other user can write to.
A socket directory anything else can write to is a squatting race, the same class as a squatted port.

**The fence must exceed the model, not the VRAM figure.** A CPU-only load (`-ngl 0`) holds the whole
file in host RAM, so an on-demand tenant carrying a 1.7 GiB model needs a cgroup limit above ~1.7 GiB;
the VRAM number that matters for a GPU load says nothing here. A 2 GiB `memory.max` silently
OOM-kills such a load and the failure reads exactly like a broken unix path.

Nothing prints "port in use" for a socket: the messages say *socket path*, and the hint an operator
needs is `ss -xlp | grep <path>`, not `fuser -n tcp`. Because the installed engine does not unlink on
bind or on exit, **demesne's launcher owns the file**: before spawning it removes a file that
nothing answers on (and only if it is a socket — a regular file is refused, not clobbered), and
`ENOENT` during that removal is success so a concurrent cleanup cannot fail a spawn. Reclaim finds
the holder in `/proc/net/unix` by path, readiness and the "is anything there" checks dial, and the
operator-facing messages say *socket path*, not port.

### Models an engine serves but cannot advertise

Routing learns a model name from the engine that advertises it on `/v1/models`. An engine that is
*down* cannot advertise, so a request naming one of its models would find the name unknown and fall
through to the router — and on 2026-11-09 that meant llama tried to load a model absent from its own
`models.ini`, failed, and restored its previous model, putting an unrelated `d1` back on the 7900 XT.

So a tenant may declare its set:

```sh
export DEMESNE_STRATA_MODELS="qwen3.8-flash-next-coder-iq1_m"     # inline
export DEMESNE_STRATA_MODELS=/etc/demesne/strata.models           # or a file, one id per line
```

The declared ids are routable even when the engine has never answered, so a stopped on-demand engine
can be *named*, which is what makes `ensure()` spawn it. A live `/v1/models` answer wins a clash,
because it is the engine's own word; the declared list is the fallback. Declaring a model the engine
no longer serves makes a request route to it and fail loudly rather than silently landing on the
router — which is the point: a wrong engine that says so beats a right-looking one that does not.

Do not try to derive this list from the engine's data directory. On this box `/srv/strata` names the
same model three different ways — directory `coder-IQ1_M`, shard `Qwen3.8-Flash-Next-GSQ-RCO-IQ1_M-00001-of-00002.gguf`,
advertised id `qwen3.8-flash-next-coder-iq1_m` — and only the last is what a request says.

### When every aux model load fails

If `llamb` refuses every model with `error while handling argument "--device": invalid device: CUDA0`
and `ggml_cuda_init: failed to initialize CUDA: unknown error`, while `nvidia-smi` works and the driver
is loaded, the problem is the CUDA device node, not demesne and not the driver:

```sh
ls -lZ /dev/nvidia-uvm          # must exist, be 0666, and be xserver_misc_device_t
sudo /etc/local.d/nvidia-uvm.start
```

On this box udev does not run: `/dev` is devtmpfs, so a device node created at runtime keeps the
default `device_t` label and nothing will relabel it. That is why CUDA cannot initialise while the
driver is perfectly healthy. `/etc/local.d/nvidia-uvm.start` is idempotent — `mknod -m 666` only
creates what is missing, `restorecon` relabels what is there — so running it by hand is the repair,
and it is also the boot hook that prevents the problem after a reboot.

Do not "clean up" a `denied { remove_name }` on that node: `nvidia-modprobe` tries to unlink and
recreate it, and the denial is the only thing keeping the correctly labelled node in place.

**The mode must be set at creation: `mknod -m 666`.** A plain `mknod` followed by `chmod` looks
equivalent and is not — `chmod` on this node's type is itself denied (it is the same `{ setattr }`
noise in the audit log), so the node stays 0644 and CUDA fails with `EACCES` on open, because it
opens the node read-write as the user. That is a *different* message from `invalid device: CUDA0`,
which is exactly why a future reader staring at a 0644 node would not connect the two.

## What is still open

- **log rotation** is real and unaddressed; the answer is probably a `logrotate` config outside
  this repo, not code.
- **sd's `--auto-fit` + VRAM wait** interact in a way that is only understood empirically: the
  code warns and continues when the card is not free in time, which is right for sd and would
  be wrong for an engine that OOMs cleanly.
- **nothing yet notices a dead engine on its own.** The measured case: llama was down from
  2026-11-02 17:34 until the next afternoon and nothing said so, because liveness was only ever
  asked at request time. `/estate/card` reports it, but the operator's `/status` still reports
  the *watch's* opinion of the router rather than the router's existence. A ticker with
  `up/down/booting` per tenant is the fix, and it is the difference between `/status` being a
  fact and being a memory.
- **the control vocabulary is the operator's, and one typed answer exists.** `/estate/card` is
  the document a client reacts to; `/status`, `/release` and `/tenants/<name>/load|stop` remain
  shaped for a human. Anything further should be asked for by a client that needs it.
- **no `restart` verb, only a restart by consequence.** `POST /tenants/<name>/stop` on a resident
  engine restarts it after `restartDelay`, which is right for an operator and wrong for a
  caller that wants it *now* (after dropping a new GGUF in the scan dir, say). The shape it
  would take: `POST /tenants/<name>/restart`, on-demand = stop then start, resident = stop and
  nudge the supervisor past its delay. The nudge is a buffered channel per tenant that
  `supervise()` selects on beside its pause — without it the caller and the supervisor race,
  and two engines fight for one port. Not built because it needs a demesne restart to exist,
  and nobody has needed "now" yet.

## Build and test

Stdlib only — no dependencies, no module cache, no network:

```sh
make            # build ./demesne, then vet and test
make build      # just the binary
make test       # go test -race ./...
make install    # ~/bin/demesne, and the ~/bin/demense spelling as a symlink
```

The tests spawn the test binary as a stand-in engine, so the parts that actually break — spawn,
readiness, idle eviction, supervisor restart, foreign-port refusal, model routing, reload, the
card document — are exercised with no GPU, no models and no network.

## Measured

Numbers from this machine — a 20 GiB RX 7900 XT, a 2 GiB GT 1030, a 4-thread Haswell:

| | |
| :-- | :-- |
| llama router, cold start to `listening` | **41 s** (it loads no model until asked) |
| cold render, 20 steps | **508.6 s** (weights still page-cached) |
| cold render, **1** step | **706.6 s** |
| warm render | ~148.6 s ≈ 20 steps × 7 s — **pure sampling** |
| eviction on a chat request | child stopped, card released **before** the router was asked: **12.136 → 0.026 GiB in 1 s** |
| idle release | fired **exactly** on the clock, then 12.136 → 0.026 GiB |

The 1-step run being *slower* than the 20-step run is the interesting one: **the cost is the
load, not the sampling.** ~700 s is weights and init whatever the step count, which is why the
idle timeout is the lever that matters.

## Design notes worth keeping

Bugs found while building this, kept because the next reader will otherwise find them again:

1. **The idle timer must be armed from the *end* of a request, not the start.** A cold render
   can take ten minutes; a five-minute timer armed when the child is ensured kills it mid-load.
2. **The child must be killed on SIGTERM.** It is in its own process group (`Setpgid`), which
   means it does *not* die when demesne does. Without a handler, a service restart orphans
   ~12.5 GiB that nothing owns.
3. **A status surface must not take the transition lock**, or `/status` blocks for the whole
   eight minutes it takes to describe.
4. **A child that dies during startup is not a child that is still loading.** Waiting for a port
   alone cannot tell the two apart, and the difference is fifteen minutes of someone's
   afternoon.
5. **An env key built from a name must uppercase it.** The fence asked for
   `DEMESNE_sd_MEMORY_HIGH`, which nobody sets, so every limit in the estate was silently
   unlimited for as long as the tenant model existed. `/status` now publishes the fence it
   actually applied, which is the only reason this class of bug stays fixed.
6. **A helper looked up on `PATH` is a fence that may not exist.** A `supervise-daemon` service
   has a `PATH` without `~/bin`; the first real start ran the whole estate unlimited, with one
   log line per tenant that nobody reads.
7. **"Up" is not "holding the card".** A router with nothing loaded holds a port and about
   28 MiB. The first card document said `holders:["llama"]` for exactly that, and a client
   trusting it would refuse to render on a free card.
8. **A slow answer needs a clock on the right side of the proxy.** The client's header
   deadline runs from the first byte out, so a proxy that forwards headers only when the
   upstream sends them hands the client a deadline that the engine can exceed: 300 s against a
   cold 27B load, in the case that started this. Answer the caller first, and let the engine
   catch up.
9. **A caller that gives up is not an engine that failed.** The proxy sees a cancelled client
   and a refused connection as the same returned error, and counting the first as evidence is
   how a client timeout condemned a healthy router: on 2026-11-07 two of them five minutes
   apart tripped the breaker, the watch killed a child that was 14.65 GiB into a cold load, and
   every retry after that had no model and no chance. Cancellations are now logged and dropped;
   the breaker only hears about the engine.

## Licence

MPL-2.0 (see `LICENSE`). The Mozilla Public License is FILE-LEVEL copyleft: you may combine
this with differently-licensed code, but modifications to MPL-covered *files* must stay MPL.
