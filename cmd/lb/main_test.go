package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dangoodman/k8s-sleepy-proxy/internal/sleepy"
)

func TestLBRequiresTenantHeader(t *testing.T) {
	s := &server{}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestLBWakesColdTenantAndProxies(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Sleepy-Tenant"); got != "tenant-a" {
			t.Fatalf("X-Sleepy-Tenant = %q", got)
		}
		fmt.Fprint(w, "echo")
	}))
	defer backend.Close()

	backendURL := backend.Listener.Addr().String()
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/state/tenant-a":
			fmt.Fprint(w, `{"tenantId":"tenant-a","state":"Cold","generation":1}`)
		case "/wake/tenant-a":
			fmt.Fprintf(w, `{"tenantId":"tenant-a","state":"Running","backend":%q,"generation":2}`, backendURL)
		default:
			http.NotFound(w, r)
		}
	}))
	defer controller.Close()

	s := &server{
		controllerURL: controller.URL,
		authToken:     "test-token",
		client:        controller.Client(),
		cacheTTL:      time.Second,
		wakeTimeout:   time.Second,
		cache:         map[string]cacheEntry{},
	}
	req := httptest.NewRequest(http.MethodGet, "/hello", nil)
	req.Header.Set("TENANT", "tenant-a")
	rec := httptest.NewRecorder()

	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "echo" {
		t.Fatalf("body = %q", rec.Body.String())
	}
}

func TestResolveBackendReturnsFailedAsUnavailable(t *testing.T) {
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"tenantId":"tenant-a","state":"Failed","failureReason":"boom","generation":1}`)
	}))
	defer controller.Close()

	s := &server{
		controllerURL: controller.URL,
		authToken:     "test-token",
		client:        controller.Client(),
		cacheTTL:      time.Second,
		wakeTimeout:   time.Millisecond,
		cache:         map[string]cacheEntry{},
	}
	state, _, err := s.resolveBackend(t.Context(), "tenant-a")
	if err == nil {
		t.Fatalf("expected error, got state %+v", state)
	}
}

var _ = sleepy.StateRunning
