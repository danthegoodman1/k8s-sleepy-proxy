package store

import (
	"context"
	"testing"

	"github.com/dangoodman/k8s-sleepy-proxy/internal/sleepy"
)

func TestMemoryStoreCompareAndSwapPreventsDuplicateWake(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	tenant, err := store.UpsertTenant(ctx, sleepy.Tenant{
		TenantID:     "tenant-a",
		Image:        "example/echo:latest",
		UpstreamPort: 9000,
		IdleSeconds:  30,
	})
	if err != nil {
		t.Fatal(err)
	}

	waking, ok, err := store.CompareAndSwapState(ctx, tenant.TenantID, tenant.Generation, sleepy.StateCold, sleepy.StateWaking)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("first compare-and-swap did not win")
	}

	_, ok, err = store.CompareAndSwapState(ctx, tenant.TenantID, tenant.Generation, sleepy.StateCold, sleepy.StateWaking)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("stale compare-and-swap unexpectedly won")
	}
	if waking.State != sleepy.StateWaking {
		t.Fatalf("state = %s, want %s", waking.State, sleepy.StateWaking)
	}
}
