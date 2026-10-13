package main

import (
	"context"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/demo"
	"io"
	"testing"
	"time"
)

func TestSmoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	env, err := demo.New(t.TempDir(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := env.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := run(ctx, env); err != nil {
		t.Fatal(err)
	}
}
