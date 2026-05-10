package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dangoodman/k8s-sleepy-proxy/internal/sleepy"
	"github.com/dangoodman/k8s-sleepy-proxy/internal/store"
)

type fakeKube struct {
	created int
	deleted int
}

func (k *fakeKube) EnsureTenant(context.Context, sleepy.Tenant) (string, error) {
	k.created++
	return "sleepy-tenant-a.sleepy-system.svc.cluster.local:80", nil
}

func (k *fakeKube) WaitReady(context.Context, string, time.Duration) error {
	return nil
}

func (k *fakeKube) DeleteTenant(context.Context, string) error {
	k.deleted++
	return nil
}

func TestServerWakeAndSleepLifecycle(t *testing.T) {
	ctx := context.Background()
	tenantStore := store.NewMemoryStore()
	_, err := tenantStore.UpsertTenant(ctx, sleepy.Tenant{
		TenantID:     "tenant-a",
		Image:        "example/echo:latest",
		UpstreamPort: 9000,
		IdleSeconds:  30,
	})
	if err != nil {
		t.Fatal(err)
	}
	kube := &fakeKube{}
	server := NewServer(tenantStore, kube, time.Second, nil)

	wakeReq := httptest.NewRequest(http.MethodPost, "/wake/tenant-a", nil)
	wakeRec := httptest.NewRecorder()
	server.ServeHTTP(wakeRec, wakeReq)
	if wakeRec.Code != http.StatusOK {
		t.Fatalf("wake status = %d body=%s", wakeRec.Code, wakeRec.Body.String())
	}
	var state sleepy.StateResponse
	if err := json.NewDecoder(wakeRec.Body).Decode(&state); err != nil {
		t.Fatal(err)
	}
	if state.State != sleepy.StateRunning || state.Backend == "" {
		t.Fatalf("state after wake = %+v", state)
	}
	if kube.created != 1 {
		t.Fatalf("created = %d, want 1", kube.created)
	}

	sleepReq := httptest.NewRequest(http.MethodPost, "/sleep/tenant-a", strings.NewReader(`{"tenantId":"tenant-a"}`))
	sleepRec := httptest.NewRecorder()
	server.ServeHTTP(sleepRec, sleepReq)
	if sleepRec.Code != http.StatusOK {
		t.Fatalf("sleep status = %d body=%s", sleepRec.Code, sleepRec.Body.String())
	}
	if kube.deleted != 1 {
		t.Fatalf("deleted = %d, want 1", kube.deleted)
	}
	got, err := tenantStore.GetTenant(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != sleepy.StateCold {
		t.Fatalf("state after sleep = %s, want Cold", got.State)
	}
}
