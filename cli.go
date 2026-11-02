// cli.go -- the operator's interface. /status is the only status surface, and these
// subcommands are its client: one thing to read, one thing to ask.
//
// Why not `rc-service --user llama status` any more: an engine demesne forked is not in
// the OpenRC service tree, so rc-service can neither see nor stop it. The old way -- and
// the new one -- is written down in DESIGN.md.
package main

import (
	"flag"
	"fmt"
	"io"
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
  demesne version               print the version

Slots: llama (55555), llamb (55556), sd (55557), whisper (55559).
Every flag of the server is still accepted before the subcommand, and
` + "`DEMESNE_*`" + ` environment variables are honoured, so -listen works as usual.
`

func runSubcommand(sub string, cfg config) {
	base := "http://" + cfg.listen

	switch sub {
	case "version":
		fmt.Println("demesne " + version)
		return
	case "help", "-h", "--help":
		fmt.Print(usage)
		return
	case "status":
		body, err := call(http.MethodGet, base+"/status", nil)
		if err != nil {
			fail("status", err)
		}
		fmt.Print(body)
		return
	case "release":
		target := base + "/release"
		if args := flag.Args(); len(args) > 0 && !strings.HasPrefix(args[0], "-") {
			target += "?slot=" + args[0]
		}
		body, err := call(http.MethodGet, target, nil)
		if err != nil {
			fail("release", err)
		}
		fmt.Print(body)
		return
	case "load", "start":
		slot, ok := slotArg(sub)
		if !ok {
			return
		}
		body, err := call(http.MethodPost, base+"/slots/"+slot+"/load", nil)
		if err != nil {
			fail("load "+slot, err)
		}
		fmt.Print(body)
		return
	case "stop":
		slot, ok := slotArg(sub)
		if !ok {
			return
		}
		body, err := call(http.MethodPost, base+"/slots/"+slot+"/stop", nil)
		if err != nil {
			fail("stop "+slot, err)
		}
		fmt.Print(body)
		return
	default:
		fmt.Fprintf(os.Stderr, "demesne: unknown subcommand %q\n\n%s", sub, usage)
		os.Exit(2)
	}
}

func slotArg(sub string) (string, bool) {
	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "demesne: %s wants a slot name (llama, llamb, sd, whisper)\n", sub)
		os.Exit(2)
	}
	return args[0], true
}

// call talks to the running demesne. `load` can legitimately block for the whole cold
// load of an engine (~8 minutes for sd), so there is deliberately no client timeout:
// the operator can ^C, and a timeout here would lie about what happened.
func call(method, target string, body io.Reader) (string, error) {
	req, err := http.NewRequest(method, target, body)
	if err != nil {
		return "", err
	}
	client := &http.Client{}
	started := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("%s: %s after %s", resp.Status,
			strings.TrimSpace(string(out)), time.Since(started).Round(time.Millisecond))
	}
	return string(out), nil
}

func fail(what string, err error) {
	fmt.Fprintf(os.Stderr, "demesne: %s failed: %v\n", what, err)
	os.Exit(1)
}
