# demesne

> *demesne* — the part of a manor the lord keeps in his own hand. Here: the resident set.
> Pronounced /dɪˈmeɪn/, like "domain"; the `s` is a 16th-century scribal addition and has
> never been sounded. (`demense` is the common misspelling.)

`demesne` is the single owner of the local-inference estate on this box: it supervises and
routes the four engines, and it owns the two scarce things they contend for — the 20 GiB
card and the port numbers.

It is a fork of [`stadi`](https://github.com/bqv/stadi) at `ad49321`, which was a slot
broker in front of *two* engines. The slot broker is the hard part and it is unchanged in
spirit: what grew is that demesne now `fork/exec`s the engines it owns instead of asking
`rc-service` to do it, so there is one owner and one actor per restart. The decisions
behind that are in [DESIGN.md](DESIGN.md); this file is how to build, configure and use it.

## The estate

| engine | port | owned by demesne | policy | routed |
| :-- | --: | :-- | :-- | :-- |
| `llama` — llama.cpp router | 55555 | when adopted (else OpenRC) | resident | `/v1/*`, default |
| `llamb` — aux router, GT 1030 | 55556 | when adopted (else OpenRC) | resident | no (addressed directly) |
| `sd` — stable-diffusion.cpp | 55557 | always | on demand | `/sdapi/*`, `/v1/images/*` |
| `whisper` — whisper.cpp | 55559 | when adopted | on demand | `/inference`, `/v1/audio/*` |
| `demesne` itself | 55558 | — | — | all of the above |

`whisper` used to claim 55557 as well, as an OpenRC service beside sd, with nothing
enforcing exclusivity. It is CPU-only, so the two were never exclusive — it was a port
collision, and the fix is a port of its own (§3 of [DESIGN.md](DESIGN.md)).

## The problem

On a one-tenant card this is not a scheduling nicety, it is a correctness problem:

- The llama router keeps a resident model — 17.6–19.7 GiB of a 20 GiB card.
- A Qwen-Image DiT wants ~13 GiB, plus its VAE and a text encoder.
- **`sd-server` cannot unload.** There is no flag and no endpoint (grep
  `examples/server/*.cpp` for `unload`, `evict`, `release` — there is no such route); only
  the *process exiting* frees the weights. Measured: 12.5 GiB → 0.03 GiB.
- Neither engine knows the other exists, so whichever loads second OOMs mid-load.

So a plain always-on `sd-server` holds ~12.5 GiB until a human stops it, and **a local model
cannot generate an image by itself**: to call the image model it would have to unload the
very model making the call.

## What it does

| request | what happens |
| :-- | :-- |
| **generation** — `/v1/images/*`, `/sdapi/v1/txt2img`, `/img2img`, `/inference` | make room on the card, spawn the child, proxy to it, re-arm its idle clock |
| **chat** — everything else, e.g. `/v1/chat/completions`, `/models/load` | evict the media child, **wait for the VRAM to actually return**, proxy to the router |
| **metadata** — `/v1/models`, `/props`, `/health`, `/healthz` | proxy untouched; the router loads nothing for these, and a picker polls them constantly |
| **observation** — other `/sdapi/*` (`/progress`, `/options`, …) | proxy only if loaded, otherwise `503 sd is not loaded` |

Plus a self-inflicted `keep_alive`: after `DEMESNE_IDLE` with no media request the child is
stopped and the card frees itself. That is the feature sd.cpp does not have.

The generation/observation split is not cosmetic. Without it a monitoring loop polling
`/sdapi/v1/progress` would spawn the child and pin the card forever, defeating the idle
timer entirely.

**State needs no flags: the child's existence *is* the state.** `GET /status` reports
`sd_child_up`, so "is sd loaded?" is answered by process liveness rather than a boolean
that can desync. It also reports every slot: policy, pid, uptime, restarts, last error,
and `port_held_elsewhere` when a port is answered by something demesne does not own.

## Build and test

Stdlib only — no dependencies, no module cache, no network:

```sh
make            # build ./demesne, then vet and test
make build      # just the binary
make test       # go test -race ./...
make install    # ~/bin/demesne, and the ~/bin/demense spelling as a symlink
```

`make install` also creates `demense`, because that is how everyone spells it on the first
try. It is a symlink to the same binary, not a second name for anything else.

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

The estate, as a block of ports — the engines keep their own numbers and demesne sits on the
one every caller already means:

| | llama | llamb | sd | whisper | demesne |
| :-- | --: | --: | --: | --: | --: |
| port | 55556 | 55557 | 55558 | 55559 | 55555 |

Because one port answers for four engines, **the model name decides the engine**: demesne
reads `model` from the request body, and both routers' `/v1/models` are merged into the answer
the front door gives. A path still decides the *kind* of request (`/sdapi/*` is the media
child, `/v1/audio/*` is whisper), and a model on the small card never evicts a render from the
big one.

The cgroup tree is `/sys/fs/cgroup/estate`, with `ctl` for demesne itself,
`llama/{llm,aux}` for the two routers and `sd`/`whisper` beside them; the limits are re-written
and the children re-joined on a timer, not only at startup.

`demesne` with no subcommand runs the estate; OpenRC starts that. Per-engine `start|stop`
is demesne's now, because `rc-service` cannot see a child demesne forked. Anything that
still asks a port directly — DSH's `dsh-local` provider, for one — keeps talking to
`:55558` and notices nothing.

## Two ways in

One handler, two transports. The port is for clients configured with a URL (DSH reaches
`:55558` by name); `-socket` adds the same API on a unix socket, which is for anything
running as you:

```sh
curl --unix-socket $XDG_RUNTIME_DIR/demesne.sock http://demesne/status
demesne status          # or this, which takes the port
```

There is deliberately **no second protocol**: the socket speaks the same OpenAI dialect and
the same control endpoints, because a second dialect would mean a second implementation on
both sides, and none of it would make a five-minute render or a 20 tok/s stream faster. What
the socket buys is the absence of a port — nothing to collide with, no listener any local
process can dial, and a file mode that is the access control. See §6a of
[DESIGN.md](DESIGN.md) for why ward should use it for control and *not* for inference.

## The card, in one small document

`/status` is the operator's document and it may grow. A *client* that only wants to know
whether the card is busy gets a separate, deliberately tiny one:

```sh
curl --unix-socket $XDG_RUNTIME_DIR/demesne.sock 'http://demesne/estate/card?group=gpu'
{"group":"gpu","holders":["sd"],"since_ms":1793711314557,
 "slots":[{"name":"llama","state":"stopped"},{"name":"sd","state":"running"}]}
```

`holders` is an array because the invariant is *at most one holder at rest*: during a
handover both engines are up for the length of an eviction. `since_ms` is when the card
became busy, and `0` when that is not known (an external engine's readiness is not an
event demesne observed). `group` is the unit, not a driver's device name — see §6b.

## Configure

**Flags first, environment as the fallback.** Every setting is a command-line argument;
each falls back to `DEMESNE_<NAME>`, then to `STADI_<NAME>` (still read, with a note on
stderr), then to a portable default. `demesne -h` is the authoritative list.

| flag | environment | default |
| :-- | :-- | :-- |
| `-listen` | `DEMESNE_LISTEN` | `127.0.0.1:55555` |
| `-socket` | `DEMESNE_SOCKET` | none — same API on a unix socket, mode 0600 |
| `-llm` | `DEMESNE_LLAMA` (was `DEMESNE_LLM`) | `http://127.0.0.1:55556` — the router's URL |
| `-sd` | `DEMESNE_SD` | `http://127.0.0.1:55558` — where the **child** comes up |
| `-llamb` | `DEMESNE_LLAMB` | `http://127.0.0.1:55557` |
| `-whisper` | `DEMESNE_WHISPER` | `http://127.0.0.1:55559` |
| `-idle` | `DEMESNE_IDLE` | `5m` — evict an on-demand child after this long with no request |
| `-whisper-idle` | `DEMESNE_WHISPER_IDLE` | `10m` |
| `-busy-wait` | `DEMESNE_BUSY_WAIT` | `2m` — how long a chat request waits for a render, then 503 |
| `-vram-wait` | `DEMESNE_VRAM_WAIT` | `45s` — bound on waiting for the card to come back |
| `-load-wait` | `DEMESNE_LOAD_WAIT` | `15m` — bound on a child's port appearing |
| `-free-gib` | — | `2` — treat the card as free below this many GiB used |
| `-restart-delay` | `DEMESNE_RESTART_DELAY` | `5s` — first delay between resident restarts |
| `-log-dir` | `DEMESNE_LOG_DIR` | `$XDG_CACHE_HOME/demesne` — one `<slot>.{log,err}` pair per engine |
| `-sd-bin` | `DEMESNE_SD_BIN` | the `sd-server` found on `PATH` |
| `-sd-args` | `DEMESNE_SD_ARGS` | **required** — the child's whole command line |
| `-llama-bin` / `-llamb-bin` / `-whisper-bin` | `DEMESNE_<SLOT>_BIN` | **empty: leave that engine to OpenRC** |
| `-<slot>-args` | `DEMESNE_<SLOT>_ARGS` | the child's command line |
| `-<slot>-env` | `DEMESNE_<SLOT>_ENV` | extra `KEY=VALUE` for that child, e.g. its Vulkan pin |
| `-<slot>-dir` | `DEMESNE_<SLOT>_DIR` | the child's working directory — a router's `models.ini` names weights relative to it |
| `-conf` | `DEMESNE_CONF` | the file `SIGHUP`/`POST /reload` re-reads; unset = nothing to reload from |
| `-llm-service` | `DEMESNE_LLM_SERVICE` | `llama` — legacy: the OpenRC service to restart |
| `-llm-restart-wait` | `DEMESNE_LLM_RESTART_WAIT` | `3m` — legacy |
| `-restart-window` / `-restart-max` | `DEMESNE_RESTART_*` | `30m` / `2` |
| `-breaker` / `-fail-threshold` | `DEMESNE_BREAKER` / `DEMESNE_FAIL_THRESHOLD` | `20s` / `2` |
| `-cgroup-root` | `DEMESNE_CGROUP_ROOT` | `/sys/fs/cgroup/estate` |
| `-cgroup-prepare` / `-cgroup-join` | `DEMESNE_CGROUP_*` | `cgroup-prepare`/`cgroup-join` on `PATH`, then `~/bin`; only the privileged fallback |
| `-version` | — | print the version and exit |

Memory fences are environment-only, three per slot, because they are set once and there
are twelve of them: `DEMESNE_{SD,LLAMA,LLAMB,WHISPER}_{MEMORY_HIGH,MEMORY_MAX,SWAP_HIGH}`.

There is deliberately **no hardcoded home path** anywhere: binaries are looked up on
`PATH` and the log directory follows XDG, so the tool does not assume whose machine it is
on.

Two notes on the list-valued settings. They are space-separated strings — a list, so the
environment is the natural transport for it, and a unit file quoting one through
`command_args` would need nested quotes OpenRC does not interpret. And for
stable-diffusion.cpp `-sd-args` must **not** include `--params-backend`: that flag's own
`--help` says an explicit `--params-backend` **disables `--auto-fit`**, which is what
performs the graph segmentation tiling a VAE decode too large for the card. Pinning the
*params* split throws the fallback away; pinning `--backend` is safe.

## Measured

Numbers from the machine it was written for — a 20 GiB RX 7900 XT, 4-thread Haswell, Vulkan:

| | |
| :-- | :-- |
| cold render, 20 steps | **508.6 s** (weights still page-cached) |
| cold render, **1** step | **706.6 s** |
| warm render | ~148.6 s ≈ 20 steps × 7 s — **pure sampling** |
| idle release | fired **exactly** 5 min after the render ended; **12.136 → 0.026 GiB in 1 s** |
| eviction on a chat request | child stopped and card released **before** the router was asked; the chat then answered in 51 s |

The 1-step run being *slower* than the 20-step run is the interesting one: **the cost is the
load, not the sampling.** ~700 s is weights and init whatever the step count, which is why
the idle timeout is the lever that matters rather than the step count.

## Design notes worth keeping

Bugs found while building stadi, and kept here because the next reader will otherwise find
them again:

1. **The idle timer must be armed from the *end* of a request, not the start.** It is first
   armed when the child is ensured — before the render — and a cold render can take ten
   minutes. A five-minute timer armed there kills the child mid-load. It re-arms when the
   request completes, and refuses to fire while a generation is in flight.
2. **The child must be killed on SIGTERM.** It is started in its own process group
   (`Setpgid`) so that stopping it takes any children with it — which also means it does
   *not* die when demesne does. Without a signal handler, a service restart orphans ~12.5
   GiB that nothing owns any more.
3. **A status surface must not take the transition lock.** The old broker held one mutex
   across a cold load, so `/status` blocked for the whole eight minutes it took to describe.
   State lives behind per-slot mutexes now; the transition lock is only for transitions.
4. **A child that dies during startup is not a child that is still loading.** Waiting for a
   port alone cannot tell the two apart, and the difference is fifteen minutes of someone's
   afternoon.

And one operational trap: a `supervise-daemon` user service is stopped with SIGTERM, so a
broker that spawns long-lived children **must** handle that signal. Cheap to do, expensive
to skip.

## Licence

MPL-2.0 (see `LICENSE`). The Mozilla Public License is FILE-LEVEL copyleft: you may combine
this with differently-licensed code, but modifications to MPL-covered *files* must stay MPL.
