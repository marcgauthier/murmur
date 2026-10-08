package rime_test

import (
	"context"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/internal/rimequal"
)

func TestLiveMaintainedViews(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	sample, err := rimequal.Run(ctx, rimequal.Config{Views: true, Rows: 100, Shards: 4, Readers: 4, Writers: 4, Seed: 127, Duration: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if sample.ViewChecks == 0 {
		t.Fatal("no view comparisons")
	}
	t.Logf("%d view checks, %d writes, %d reads; heap %d bytes", sample.ViewChecks, sample.Writes, sample.Reads, sample.HeapBytes)
}
