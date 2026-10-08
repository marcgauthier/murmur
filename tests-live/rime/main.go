// Command rime runs standalone in-memory engine qualification.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/marcgauthier/murmur/internal/rimequal"
)

func main() {
	views := flag.Bool("views", false, "maintain and verify cached filters and joins")
	duration := flag.Duration("duration", 30*time.Minute, "measured workload duration (excluding seeding)")
	rows := flag.Int("rows", 1000, "rows in each of two tables")
	shards := flag.Int("shards", 64, "shards per table")
	readers := flag.Int("readers", 4, "concurrent snapshot readers")
	writers := flag.Int("writers", 4, "contending writers")
	seed := flag.Int64("seed", 1, "reproducible worker random seeds")
	interval := flag.Duration("sample-interval", 10*time.Second, "JSON measurement interval")
	flag.Parse()
	if *duration <= 0 {
		fmt.Fprintln(os.Stderr, "duration must be positive")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *duration+10*time.Minute)
	defer cancel()
	_, err := rimequal.Run(ctx, rimequal.Config{Views: *views, Rows: *rows, Shards: *shards, Readers: *readers, Writers: *writers, Seed: *seed, SampleInterval: *interval, Duration: *duration, Output: os.Stdout})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
