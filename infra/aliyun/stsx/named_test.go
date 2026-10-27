package stsx

import (
	"context"
	"errors"
	"testing"

	"github.com/bpcoder16/pixiu/infra/internal/named"
)

func TestNamedLifecycle(t *testing.T) {
	old := namedClients
	namedClients = named.New[*Client]("stsx")
	t.Cleanup(func() {
		_ = CloseAll()
		namedClients = old
	})
	independent := newTestClient(t, testConfig(""))
	cfg := testConfig("")
	c, err := NewDefault(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if Default() != c || Named(cfg.Name) != c {
		t.Fatal("default alias mismatch")
	}
	if _, err := NewNamed(cfg); err == nil {
		t.Fatal("duplicate accepted")
	}
	if err := CloseAll(); err != nil {
		t.Fatal(err)
	}
	if err := CloseAll(); err != nil {
		t.Fatal(err)
	}
	if _, err := NewNamed(cfg); err == nil {
		t.Fatal("registry reopened")
	}
	ctx := context.Background()
	_, err = c.AssumeRole(ctx, testRequest())
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("registered client not closed: %v", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	c = independent
	_, err = c.AssumeRole(ctx, testRequest())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("independent client closed: %v", err)
	}
}
