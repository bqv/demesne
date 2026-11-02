# HANDOFF — demesne

**Status:** fork created 2026-11-02 from [`stadi`](https://github.com/bqv/stadi) at commit
`ad49321`. The **remit is agreed; the specification is not.** The code in this tree is still
stadi's code, unchanged apart from the module name and the banner in `README.md`. Treat this
document as the input to the drill-down, not as a design.

**Name:** *demesne* — the part of a manor the lord keeps in his own hand. In this metaphor:
the resident set. Pronounced /dɪˈmeɪn/ (same as "domain"; the `s` is a 16th-century scribal
addition and has never been sounded). `demense` is the common misspelling — ship a symlink for
it. Never abbreviate to `dm` (display manager) or `dmn`.

---

## 1. Remit

`demesne` is to be the single owner of the local-inference estate on this box:

- **supervise and route all four engines** — `llama` (main router, `127.0.0.1:55555`),
  `llamb` (aux router, `55556`), `sd` (`sd-server`, `55557`), `whisper` (`55557`);
- **keep stadi's original duties** — reverse proxy in front of the router, arbitration of the
  single 20 GiB card, idle eviction of the media child, the LLM breaker, `/status`.

One process owns the card and the ports, instead of stadi owning two engines while OpenRC +
supervise-daemon own the other two.

Test of scope: *does the thing hold or contend for a scarce resource (VRAM, 55557, the card)?*
If yes, it belongs here. Sessions belong to `ward`; messaging belongs to `openmessage`/`rcsagent`.

## 2. Settled

- Name, repo location (`~/prj/demesne`), fork point (`ad49321`), module renamed to `demesne`.
- Go, **stdlib only** — inherited constraint, keep it. (HTTP proxy + fork/exec + one binary;
  nothing here needs a dependency.)
- It owns *exclusive resources*, not just one process tree.
- It absorbs stadi's function; DSH keeps talking to `:55558`.

## 3. NOT settled — the drill-down

Every one of these is a real fork in the design; do not assume an answer:

1. **Process model.** Does demesne `fork/exec` llama and llamb itself, or keep them as OpenRC
   services and merely ask `rc-service` to restart them (what stadi does today)?
2. **Slot model.** One GPU slot (llama vs sd) plus one media port (55557: sd vs whisper)? Or a
   generic slot table — name, port, VRAM ceiling, exclusivity group, idle policy, child spec?
3. **sd vs whisper.** They both claim 55557 but whisper is CPU-only. Are they genuinely
   exclusive, or is this only a port collision to be resolved once?
4. **cgroups.** Per-service limits live in the OpenRC init scripts today
   (`init.d/{dsh,llama,llamb,stadi}` + `sudo -n cgroup-prepare/cgroup-join`). Move into demesne,
   or leave?
5. **Restart policy per child.** Eager respawn with backoff, or lazy on-demand (what the sd
   child does now)? How does that compose with the now-unlimited `supervise-daemon` respawns?
6. **Operator interface.** Does `/status` become the only status surface? What happens to
   `rc-status --user`, `rc-update`, `rc-service` as the way you stop/start one engine?
7. **What stays an OpenRC service at all?** (`dsh`, `ward`, `gpg-agent`, `openmessage`,
   `rcsagent`, `adb` are surely out of remit.)
8. **Migration order.** sd first, whisper second, llama/llamb last — or never?
9. **CLI surface.** Subcommands (`demesne status|load|release|shunt`) vs flags? `/status` JSON
   schema? Does `stadi`'s flag/env set survive under new names (`DEMESNE_*`)?
10. **Config.** One `conf.d/demesne` for the whole estate, or keep per-engine `conf.d` sourced by
    the init script as today?
11. **Transition and rollback.** Does stadi stay deployed on a different port while demesne is
    built? What is the rollback if demesne misbehaves?
12. **Version line.** `version` const is still `0.2.0`; pick a fork version.

## 4. What the code does today (inherited from stadi, unchanged)

- `main.go` (687 lines) — flag/env parsing, the broker, routing, `/status`, child
  spawn/reap/stop, idle timer, `/release`.
- `watch.go` (213 lines) — `llmWatch`: breaker, restart budget, orphan detection, and
  `recoverLLM()` which calls `rc-service --user llama restart`.
- `go.mod` — `module demesne`, `go 1.27`, no dependencies.
- Own binary at `~/bin/stadi` (11 MB, built 26 Oct, go1.27.2); `demesne` is not installed yet.
- **Identity strings are still `stadi`**, deliberately left alone in the snapshot: `-version`
  prints `stadi 0.2.0`, `log.SetPrefix("stadi: ")`, the HTTP error bodies, the `defaultSDBin`
  / `defaultLogDir` fallbacks, and the `STADI_*` env names that `conf.d/stadi` exports. Renaming
  the binary means renaming those env vars and the init script's `command_args` in the same
  commit, or keeping `STADI_*` as compatibility aliases.

Defaults, from `main.go`:

| flag | env | default |
|---|---|---|
| `-listen` | `STADI_LISTEN` | `127.0.0.1:55558` |
| `-llm` | `STADI_LLM` | `http://127.0.0.1:55555` |
| `-sd` | `STADI_SD` | `http://127.0.0.1:55557` |
| `-idle` | `STADI_IDLE` | `5m` (conf.d sets `30m`) |
| `-busy-wait` | `STADI_BUSY_WAIT` | `2m` |
| `-vram-wait` | `STADI_VRAM_WAIT` | `45s` |
| `-load-wait` | `STADI_LOAD_WAIT` | `15m` |
| `-free-gib` | — | `2` |
| `-sd-bin`, `-sd-args`, `-sd-log-dir` | `STADI_SD_*` | — |
| `-llm-service` | `STADI_LLM_SERVICE` | `llama` |
| `-llm-restart-wait` | `STADI_LLM_RESTART_WAIT` | `3m` |
| `-restart-window` / `-restart-max` | `STADI_RESTART_*` | `30m` / `2` |
| `-breaker` / `-fail-threshold` | `STADI_BREAKER` / `STADI_FAIL_THRESHOLD` | `20s` / `2` |
| `-rc-service` | `STADI_RC_SERVICE` | resolved via `LookPath` |

Routes: `/status`, `/release`; generation (`/v1/images/*`, `/sdapi/v1/txt2img|img2img`) spawns
the child and proxies to it; observation (`/sdapi/*`) reports only, never spawns; metadata
(`/v1/models`, `/props`, `/health`, `/healthz`) proxies without a transition; **everything else**
goes to the llama proxy. `slotName()` is a bool — `"sd"` or `"llm"`.

## 5. The estate as it stands

| engine | service | port | supervised by | logs |
|---|---|---|---|---|
| llama (router) | `init.d/llama` | 55555 | supervise-daemon | `~/.dsh/logs/llama.{log,err}` |
| llamb (aux router) | `init.d/llamb` | 55556 | supervise-daemon | `llamb.{log,err}` |
| sd-server | **none** — child of stadi | 55557 | stadi (`ensureChild`/`stopChild`) | `stadi-sd.{log,err}` |
| whisper | `init.d/whisper` | 55557 | supervise-daemon | `whisper.{log,err}` |
| stadi | `init.d/stadi` | 55558 | supervise-daemon | `stadi.{log,err}` |
| openmessage | `init.d/openmessage` | 50057 | supervise-daemon | `openmessage.{log,err}` |
| dsh | `init.d/dsh` | 8081 (+8080 auth) | supervise-daemon | `dsh.{log,err}` |
| ward | `init.d/ward` | socket | supervise-daemon | `ward.{log,err}` |

Respawn settings as of 2026-11-02 (unlimited): llama/llamb/stadi **5 s**; dsh/ward **15 s**;
gpg-agent/openmessage/rcsagent/adb **30 s**. whisper is stopped and in no runlevel.

## 6. Known problems this fork should fix

1. **55557 collision.** sd (stadi's child) and whisper (an OpenRC service) both bind
   `127.0.0.1:55557`. Nothing enforces exclusivity; stadi only refuses a *foreign* holder at the
   moment it tries to spawn (`ensureChild`).
2. **whisper is not routable through stadi.** `/inference` matches no route, so the default
   branch proxies it to **llama**. Either whisper gets a route or it stays direct-only.
3. **Double restart.** `recoverLLM()` restarts llama via `rc-service` after 2 proxy failures —
   including the case "port dead, VRAM free", which supervise-daemon is already respawning
   (now unlimited, every 5 s). Two actors, one event.
4. **The sd child has no backoff.** A crash is logged and marked down; the next media request
   retries immediately, and each failed attempt can pay `load-wait` (15 m) of cold load.
5. **No syslog.** `/dev/log` is absent; supervise-daemon's own stdio is `/dev/null`. Its
   give-up/restart lines go nowhere. No logrotate for `~/.dsh/logs` (llamb.err is ~140 MB).
6. **Orphaned VRAM.** A dead router still holding VRAM cannot be fixed by a restart; the code
   marks it `orphaned` and freezes the LLM side for 24 h pending a human.

## 7. Constraints

- Runs as a `--user` service: no root, except `sudo -n` helpers (`~/bin/cgroup-prepare`,
  `cgroup-join`).
- OpenRC 0.64.1: `respawn_max=0` = unlimited; `respawn-period` defaults 12 s,
  `respawn-delay-cap` 30 s; the supervisor's stdio is redirected to `/dev/null` at fork.
- pidfiles and the service tree live under `XDG_RUNTIME_DIR` (`/run/user/1000`,
  `/run/user/1000/openrc`).
- No `/dev/log`, no logrotate, no journal.
- Go stdlib only; British-neutral spelling in code and docs (`harbor` was rejected deliberately).
- The 7900 XT is 20 GiB and single-tenant: every design choice here is about *who may hold it*.

## 8. Pointers

- Fork point: `ad49321` in this repo. `origin` still points at `github.com/bqv/stadi` —
  re-point or remove before any push.
- Live sibling: `~/prj/stadi` (still the deployed code), `~/bin/stadi`.
- Init scripts: `~/etc/init.d/{llama,llamb,stadi,whisper,openmessage,dsh,ward}` and the matching
  `~/etc/conf.d/*`. `~/.config/rc/init.d` and `conf.d` are symlinks into `~/etc`.
- `~/prj/ward` — sessions; out of remit, but the naming sibling.

## 9. First tasks for the drill-down

1. Answer §3.1 and §3.2 (process model, slot model) — everything else depends on them.
2. Sketch the child-spec struct (name, binary, argv, port, VRAM class, exclusivity group,
   idle policy, cgroup, log paths, restart policy).
3. Decide the sd/whisper port story (§3.3) — it is the cheapest concrete win.
4. Only then start the migration, and only with `sd`.
5. Rename the runtime identity and settle the `STADI_*` → `DEMESNE_*` question (§4), so the
   first deployed build does not log under the old name.
