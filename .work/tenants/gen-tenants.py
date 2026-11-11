#!/usr/bin/env python3
import os, sys

SHORT = {"model": "-m", "ctx-size": "-c", "n-gpu-layers": "-ngl", "parallel": "-np",
         "batch-size": "-b", "ubatch-size": "-ub", "flash-attn": "-fa"}
BARE = {"jinja", "embeddings", "reranking", "metrics", "cont-batching", "mlock", "no-mmap"}
TOGGLE = {"fit"}
SKIP = {"load-on-startup"}

LLM_INI, LLM_DIR, LLM_SOCK = "/srv/model/llm/models.ini", "/srv/model/llm", "/home/user/tmp/demesne-llm/models"
AUX_INI, AUX_DIR, AUX_SOCK = "/srv/model/aux/models.ini", "/srv/model/aux", "/home/user/tmp/hnet/models"


def parse(path):
    rungs, glob, cur = {}, {}, None
    for line in open(path):
        s = line.strip()
        if not s or s[0] in "#;":
            continue
        if s.startswith("[") and s.endswith("]"):
            cur = s[1:-1]
            if cur == "*":
                glob, cur = {}, None
            else:
                rungs[cur] = {}
            continue
        if "=" not in s:
            continue
        k, v = s.split("=", 1)
        k, v = k.strip().lower(), v.strip()
        if cur is None:
            glob[k] = v
        else:
            rungs[cur][k] = v
    return rungs, glob


def rescope(v, base):
    return v if v.startswith("/") else os.path.normpath(os.path.join(base, v))


def args_for(kv, glob, base, alias):
    merged = dict(glob)
    merged.update(kv)
    out = ["--device", merged["device"], "-m", rescope(merged["model"], base)] if "device" in merged else ["-m", rescope(merged["model"], base)]
    for k in sorted(merged):
        if k in SKIP or k == "device":
            continue
        v = merged[k]
        if k == "model":
            continue
        elif k in ("chat-template-file", "mmproj", "grammar-file", "lora"):
            out += ["--" + k, rescope(v, base)]
        elif k in BARE:
            if v.lower() in ("1", "on", "true", "yes"):
                out.append("--" + k)
        elif k in SHORT:
            out += [SHORT[k], v]
        else:
            out += ["--" + k, v]
    return out


def vram(path, base, overhead=900):
    p = rescope(path, base)
    try:
        return int(os.path.getsize(p) / 1048576) + overhead
    except OSError:
        return 4000


def emit(out, path, base, sockdir, group, fence, resident, overhead=900, keep_all=False, boot_keep=()):
    rungs, glob = parse(path)
    for name, kv in rungs.items():
        args = args_for(kv, glob, base, name) + [
            "--host", os.path.join(sockdir, name + ".sock"), "--alias", name]
        policy = resident.get(name, "keep" if keep_all else "on-demand")
        if policy in ("resident", "keep"):
            idle = "0"
        else:
            idle = "45m" if name == "Qwen3.8-27B-abliterated-180k" else ("30m" if name == "Kev-0.8B-Q8_0" else "5m")
        out.write("[%s]\n" % name)
        out.write("url = unix:%s\n" % os.path.join(sockdir, name + ".sock"))
        out.write("bin = /usr/bin/llama-server\n")
        out.write("args = %s\n" % " ".join(args))
        out.write("models = %s\n" % name)
        out.write("group = %s\n" % group)
        out.write("policy = %s\n" % policy)
        want_boot = policy == "resident" or name in boot_keep
        out.write("start = %s\n" % ("boot" if want_boot else "demand"))
        out.write("vram_mb = %d\n" % vram(kv.get("model", ""), base, overhead))
        out.write("idle = %s\n" % idle)
        out.write("memory_high = %s\n" % fence[0])
        out.write("memory_max = %s\n" % fence[1])
        out.write("\n")


def main():
    with open(sys.argv[1], "w") as out:
        emit(out, LLM_INI, LLM_DIR, LLM_SOCK, "gpu", ("6G", "8G"), {}, 900, keep_all=True)
        emit(out, AUX_INI, AUX_DIR, AUX_SOCK, "aux", ("2G", "3G"),
             {"Qwen3-Embedding-0.6B-Q8_0": "resident", "Kev-0.8B-Q8_0": "keep"}, 400,
             boot_keep={"Kev-0.8B-Q8_0"})
        out.write("[llama]\nenabled = false\n\n[llamb]\nenabled = false\n")


if __name__ == "__main__":
    main()
