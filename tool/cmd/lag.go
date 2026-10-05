package cmd

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/marcgauthier/murmur/tool/format"
)

type LagCommand struct{}

func (c *LagCommand) Name() string        { return "lag" }
func (c *LagCommand) Description() string { return "Report replication lag matrix across peers" }
func (c *LagCommand) Usage() string {
	return "murmur lag <node-url> [--cert=...] [--key=...] [--ca=...]"
}

func init() {
	Register(&LagCommand{})
}

func (c *LagCommand) Run(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	var target string
	for _, arg := range args {
		if !strings.HasPrefix(arg, "-") && target == "" {
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

	st, err := client.AdminStatus(ctx)
	if err != nil {
		return fmt.Errorf("fetch replication status: %w", err)
	}

	if globalOpts.JSON {
		return format.RenderJSON(stdout, st, true)
	}

	fmt.Fprintf(stdout, "=== Replication Lag Matrix: %s ===\n", target)
	var pairs [][2]string
	for k, v := range st {
		kLower := strings.ToLower(k)
		if strings.Contains(kLower, "lag") || strings.Contains(kLower, "seq") || strings.Contains(kLower, "watermark") || strings.Contains(kLower, "hlc") {
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
