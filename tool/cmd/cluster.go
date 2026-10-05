package cmd

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/marcgauthier/murmur/tool/format"
)

type StatusCommand struct{}

func (c *StatusCommand) Name() string        { return "status" }
func (c *StatusCommand) Description() string { return "Check live node health, memory, and scheduler" }
func (c *StatusCommand) Usage() string {
	return "murmur status <node-url> [--cert=...] [--key=...] [--ca=...]"
}

type ClusterCommand struct{}

func (c *ClusterCommand) Name() string        { return "cluster" }
func (c *ClusterCommand) Description() string { return "Query cluster membership and SWIM gossip state" }
func (c *ClusterCommand) Usage() string {
	return "murmur cluster <node-url> [--cert=...] [--key=...] [--ca=...]"
}

func init() {
	Register(&StatusCommand{})
	clusterCmd := &ClusterCommand{}
	Register(clusterCmd)
	Register(&aliasCmd{Command: clusterCmd, name: "members"})
}

func (c *StatusCommand) Run(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
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
		return fmt.Errorf("fetch status: %w", err)
	}

	if globalOpts.JSON {
		return format.RenderJSON(stdout, st, true)
	}

	fmt.Fprintf(stdout, "=== Murmur Node Status: %s ===\n", target)
	var pairs [][2]string
	for k, v := range st {
		pairs = append(pairs, [2]string{k, fmt.Sprintf("%v", v)})
	}
	format.RenderKV(stdout, pairs)
	return nil
}

func (c *ClusterCommand) Run(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
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

	peers, err := client.DebugPeers(ctx)
	if err != nil {
		// Fall back to status if debug/peers is not available
		st, errStatus := client.Status(ctx)
		if errStatus != nil {
			return fmt.Errorf("fetch cluster peers: %w (status: %v)", err, errStatus)
		}
		if globalOpts.JSON {
			return format.RenderJSON(stdout, st, true)
		}
		fmt.Fprintf(stdout, "=== Cluster Membership: %s ===\n", target)
		var pairs [][2]string
		for k, v := range st {
			if strings.Contains(strings.ToLower(k), "peer") || strings.Contains(strings.ToLower(k), "member") {
				pairs = append(pairs, [2]string{k, fmt.Sprintf("%v", v)})
			}
		}
		format.RenderKV(stdout, pairs)
		return nil
	}

	if globalOpts.JSON {
		return format.RenderJSON(stdout, peers, true)
	}

	fmt.Fprintf(stdout, "=== Cluster Membership (%d peers) ===\n", len(peers))
	if len(peers) > 0 {
		headers := []string{"Node ID", "Address", "Connected", "Agreed", "State"}
		var rows [][]string
		for _, p := range peers {
			rows = append(rows, []string{
				fmt.Sprintf("%v", p["node_id"]),
				fmt.Sprintf("%v", p["addrs"]),
				fmt.Sprintf("%v", p["connected"]),
				fmt.Sprintf("%v", p["schema_agreed"]),
				fmt.Sprintf("%v", p["state"]),
			})
		}
		format.RenderTable(stdout, headers, rows, globalOpts.Markdown)
	}
	return nil
}
