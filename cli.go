// cli.go -- the operator's interface. /status is the only status surface, and these
// subcommands are its client: one thing to read, one thing to ask.
//
// Why not `rc-service --user llama status` any more: an engine demesne forked is not in
// the OpenRC service tree, so rc-service can neither see nor stop it. The old way -- and
// the new one -- is written down in README.md.
//
// When a unix socket is configured the CLI prefers it and falls back to the port: the
// socket is the better transport for a caller we own (no port to contend for, mode 0600),
// but a CLI that refused to work because one of the two ways in was missing would be worse
// than either.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

const usage = `demesne -- one owner for the box's local-inference estate

  demesne                       run the estate (OpenRC starts this)
  demesne status                the whole estate, from /status
  demesne release [slot]        evict one on-demand engine, or all that are up
  demesne load <slot>           start an engine and wait for its port
  demesne stop <slot>           stop an engine
  demesne reload                re-read the config file; only what changed is touched
  demesne version               print the version

The estate (SIGHUP re-reads the same file, POST /reload does it from the socket):

  demesne 55555   llama 55556   llamb 55557   sd 55558   whisper 55559
Every flag of the server is still accepted before the subcommand, and
` + "`DEMESNE_*`" + ` environment variables are honoured, so -listen and -socket work as usual.
`

// wayIn is one transport the CLI can reach the daemon by.
type wayIn struct {
	client *http.Client
	base   string
	how    string
}

// waysIn lists them in preference order: the socket first (when configured), then the
// port. The port is what DSH uses, so it is always right to fall back to it.
func waysIn(cfg config) []wayIn {
	var out []wayIn
	if cfg.socket != "" {
		path := cfg.socket
		out = append(out, wayIn{
			base: "http://demesne",
			how:  "unix:" + path,
			client: &http.Client{Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "unix", path)
				},
			}},
		})
	}
	if cfg.listen != "" {
		out = append(out, wayIn{
			base:   "http://" + cfg.listen,
			how:    cfg.listen,
			client: &http.Client{},
		})
	}
	return out
}

func runSubcommand(sub string, cfg config) {
	switch sub {
	case "version":
		fmt.Println("demesne " + version)
		return
	case "help", "-h", "--help":
		fmt.Print(usage)
		return
	case "status", "release", "load", "start", "stop", "reload":
	default:
		fmt.Fprintf(os.Stderr, "demesne: unknown subcommand %q\n\n%s", sub, usage)
		os.Exit(2)
	}

	method, path := http.MethodGet, "/status"
	switch sub {
	case "release":
		path = "/release"
		if args := flag.Args(); len(args) > 0 && !strings.HasPrefix(args[0], "-") {
			path += "?slot=" + args[0]
		}
	case "load", "start":
		method, path = http.MethodPost, "/slots/"+slotArg(sub)+"/load"
	case "stop":
		method, path = http.MethodPost, "/slots/"+slotArg(sub)+"/stop"
	case "reload":
		method, path = http.MethodPost, "/reload"
	}

	body, errs := call(cfg, method, path)
	if body == "" {
		fail(sub, errs)
	}
	fmt.Print(body)
}

func slotArg(sub string) string {
	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "demesne: %s wants a slot name (llama, llamb, sd, whisper)\n", sub)
		os.Exit(2)
	}
	return args[0]
}

// call tries each way in until one answers. `load` can legitimately block for the whole
// cold load of an engine (~8 minutes for sd), so there is deliberately no client timeout:
// the operator can ^C, and a timeout here would lie about what happened.
func call(cfg config, method, path string) (string, []string) {
	var errs []string
	for _, w := range waysIn(cfg) {
		req, err := http.NewRequest(method, w.base+path, nil)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", w.how, err))
			continue
		}
		started := time.Now()
		resp, err := w.client.Do(req)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", w.how, err))
			continue
		}
		out, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", w.how, readErr))
			continue
		}
		if resp.StatusCode >= 400 {
			// The daemon answered; a 4xx/5xx is its verdict, not a transport failure, so
			// trying the other way in would only repeat it.
			return "", []string{fmt.Sprintf("%s via %s after %s", resp.Status,
				w.how, time.Since(started).Round(time.Millisecond))}
		}
		return string(out), nil
	}
	return "", errs
}

func fail(what string, errs []string) {
	fmt.Fprintf(os.Stderr, "demesne: %s failed: %s\n", what, strings.Join(errs, "; "))
	os.Exit(1)
}
