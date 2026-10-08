package cmd

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/marcgauthier/murmur/tool/format"
)

type GCCommand struct{}

func (c *GCCommand) Name() string        { return "gc" }
func (c *GCCommand) Description() string { return "Inspect GC state and retention watermarks" }
func (c *GCCommand) Usage() string {
	return "murmur gc <node-url> [--trigger] [--cert=...] [--key=...] [--ca=...]"
}

func init() {
	Register(&GCCommand{})
}

func (c *GCCommand) Run(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	var target string
	trigger := false

	for _, arg := range args {
		switch {
		case arg == "--trigger":
			trigger = true
		case !strings.HasPrefix(arg, "-") && target == "":
			target = arg
		}
	}

	if target == "" {
		return fmt.Errorf("missing <node-url>. Usage: %s", c.Usage())
	}

	client, err := ClientFromOpts(target, globalOpts)
	if err != nil {
		return err
	}

	if trigger {
		if err := client.TriggerGC(ctx); err != nil {
			return fmt.Errorf("trigger GC/maintenance failed: %w", err)
		}
		if globalOpts.JSON {
			return format.RenderJSON(stdout, map[string]any{"triggered": true, "status": "success"}, true)
		}
		fmt.Fprintf(stdout, "Log garbage collection and optimization triggered successfully on %s\n", target)
		return nil
	}

	st, err := client.AdminStatus(ctx)
	if err != nil {
		return fmt.Errorf("fetch GC status: %w", err)
	}

	if globalOpts.JSON {
		return format.RenderJSON(stdout, st, true)
	}

	fmt.Fprintf(stdout, "=== Garbage Collection & Retention Status: %s ===\n", target)
	var pairs [][2]string
	for k, v := range st {
		kLower := strings.ToLower(k)
		if strings.Contains(kLower, "gc") || strings.Contains(kLower, "retention") || strings.Contains(kLower, "prune") {
			pairs = append(pairs, [2]string{k, fmt.Sprintf("%v", v)})
		}
	}

	if len(pairs) == 0 {
		for k, v := range st {
			pairs = append(pairs, [2]string{k, fmt.Sprintf("%v", v)})
		}
	}

	format.RenderKV(stdout, pairs)
	return nil
}
