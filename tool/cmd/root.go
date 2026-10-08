package cmd

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/marcgauthier/murmur/tool/client"
	"github.com/marcgauthier/murmur/tool/format"
)

// Version is the Murmur CLI version.
const Version = "0.1.0"

// GlobalOptions holds flags available across all commands.
type GlobalOptions struct {
	JSON       bool
	Markdown   bool
	Verbose    bool
	CertFile   string
	KeyFile    string
	CAFile     string
	Insecure   bool
	Passphrase string
	KeyHex     string
}

// Command is the interface satisfied by all Murmur CLI subcommands.
type Command interface {
	Name() string
	Description() string
	Usage() string
	Run(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error
}

var commandRegistry = map[string]Command{}

// Register registers a subcommand.
func Register(cmd Command) {
	commandRegistry[cmd.Name()] = cmd
}

// Lookup returns the registered command with the given name.
func Lookup(name string) Command {
	return commandRegistry[name]
}

type aliasCmd struct {
	Command
	name string
}

func (a *aliasCmd) Name() string { return a.name }

// ParseGlobalOptions parses global options from args and returns the options and remaining positional args.
func ParseGlobalOptions(args []string) (GlobalOptions, []string) {
	var globalOpts GlobalOptions
	var positional []string

	i := 0
	for i < len(args) {
		arg := args[i]
		switch {
		case arg == "--json":
			globalOpts.JSON = true
		case arg == "--markdown":
			globalOpts.Markdown = true
		case arg == "--verbose":
			globalOpts.Verbose = true
		case arg == "--insecure":
			globalOpts.Insecure = true
		case strings.HasPrefix(arg, "--cert="):
			globalOpts.CertFile = strings.TrimPrefix(arg, "--cert=")
		case arg == "--cert" && i+1 < len(args):
			i++
			globalOpts.CertFile = args[i]
		case strings.HasPrefix(arg, "--key="):
			globalOpts.KeyFile = strings.TrimPrefix(arg, "--key=")
		case arg == "--key" && i+1 < len(args):
			i++
			globalOpts.KeyFile = args[i]
		case strings.HasPrefix(arg, "--ca="):
			globalOpts.CAFile = strings.TrimPrefix(arg, "--ca=")
		case arg == "--ca" && i+1 < len(args):
			i++
			globalOpts.CAFile = args[i]
		case strings.HasPrefix(arg, "--passphrase="):
			globalOpts.Passphrase = strings.TrimPrefix(arg, "--passphrase=")
		case arg == "--passphrase" && i+1 < len(args):
			i++
			globalOpts.Passphrase = args[i]
		case strings.HasPrefix(arg, "--key-hex="):
			globalOpts.KeyHex = strings.TrimPrefix(arg, "--key-hex=")
		case arg == "--key-hex" && i+1 < len(args):
			i++
			globalOpts.KeyHex = args[i]
		default:
			positional = append(positional, arg)
		}
		i++
	}
	return globalOpts, positional
}

// Execute parses args and dispatches to the requested command.
func Execute(args []string, stdout, stderr io.Writer) int {
	for _, arg := range args {
		if arg == "--version" || arg == "-v" {
			fmt.Fprintf(stdout, "murmur version %s\n", Version)
			return 0
		}
		if arg == "--help" || arg == "-h" {
			PrintUsage(stdout)
			return 0
		}
	}

	globalOpts, positional := ParseGlobalOptions(args)

	if len(positional) == 0 {
		PrintUsage(stdout)
		return 0
	}

	subcommand := positional[0]
	cmd, ok := commandRegistry[subcommand]
	if !ok {
		fmt.Fprintf(stderr, "unknown command: %q. Run 'murmur --help' for usage.\n", subcommand)
		return 1
	}

	if err := cmd.Run(context.Background(), globalOpts, positional[1:], stdout, stderr); err != nil {
		handleError(err, globalOpts, stderr)
		return 1
	}

	return 0
}

func handleError(err error, opts GlobalOptions, stderr io.Writer) {
	if opts.JSON {
		format.RenderJSONError(stderr, err)
	} else {
		fmt.Fprintf(stderr, "Error: %v\n", err)
	}
}

// PrintUsage prints global help to w.
func PrintUsage(w io.Writer) {
	fmt.Fprintf(w, `Murmur CLI - Embedded & Replicated Database Tool (v%s)

Usage:
  murmur [command] [options] [arguments]

Core Database Commands:
  init  <data-dir>                      Initialize a new encrypted database directory

Storage & Diagnostics:
  doctor  <data-dir | url>              Run automated multi-check health scorecard
  inspect <data-dir>                    Inspect offline database state, watermarks, schema
  verify  <data-dir> [--deep]           Replay and verify authoritative durable state
  repair  <data-dir> [--clear-intent] [--dry-run] Check Spool or clear a restore intent
  schema  <data-dir>                    Inspect schema manifest tables and stable column IDs
  keys    <data-dir>                    Inspect key registry and verify passphrase

Cluster & Replication Operations:
  status  <node-url>                    Check live node health, memory, scheduler
  cluster <node-url>                    Query cluster membership & SWIM gossip state
  lag     <node-url>                    Report replication lag across peers
  gc      <node-url> [--trigger]        Inspect GC watermarks and candidate batches

Backup Management:
  backup create  <data-dir> <dest.tar.gz> Create encrypted backup archive
  backup info    <archive.tar.gz>         Inspect backup metadata and table statistics
  backup verify  <archive.tar.gz>         Verify backup checksums and integrity
  backup restore <archive.tar.gz> <dir>   Restore database from backup archive

Performance & Benchmarks:
  bench  [--duration=5s]                Benchmark typed RIME writes, reads, and ciphers

Global Options:
  --json                                Output results in structured JSON format
  --markdown                            Format tables as Markdown
  --cert=<path>, --key=<path>           Client TLS certificate and key for mTLS
  --ca=<path>                           CA certificate for verifying remote nodes
  --passphrase=<str>                    Database encryption passphrase
  --key-hex=<hex>                       Database 32-byte encryption key (hex)
  -v, --version                         Print version
  -h, --help                            Print help
`, Version)
}

// ClientFromOpts builds an API client from global options and a target URL.
func ClientFromOpts(target string, opts GlobalOptions) (*client.Client, error) {
	return client.NewClient(client.ClientOptions{
		BaseURL:  target,
		CertFile: opts.CertFile,
		KeyFile:  opts.KeyFile,
		CAFile:   opts.CAFile,
		Insecure: opts.Insecure,
	})
}
