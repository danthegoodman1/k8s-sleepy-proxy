package controller

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/dangoodman/k8s-sleepy-proxy/internal/sleepy"
)

type KubeManager interface {
	EnsureTenant(context.Context, sleepy.Tenant) (string, error)
	WaitReady(context.Context, string, time.Duration) error
	DeleteTenant(context.Context, string) error
}

type Server struct {
	store       sleepy.TenantStore
	kube        KubeManager
	wakeTimeout time.Duration
	logger      *slog.Logger
}

func NewServer(store sleepy.TenantStore, kube KubeManager, wakeTimeout time.Duration, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		store:       store,
		kube:        kube,
		wakeTimeout: wakeTimeout,
		logger:      logger,
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/healthz":
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/tenants/"):
		s.handleUpsertTenant(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/state/"):
		s.handleState(w, r)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/wake/"):
		s.handleWake(w, r)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/sleep/"):
		s.handleSleep(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleUpsertTenant(w http.ResponseWriter, r *http.Request) {
	tenantID := strings.TrimPrefix(r.URL.Path, "/tenants/")
	if err := sleepy.ValidateTenantID(tenantID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var t sleepy.Tenant
	if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	t.TenantID = tenantID
	if t.Image == "" {
		http.Error(w, "image is required", http.StatusBadRequest)
		return
	}
	if t.UpstreamPort == 0 {
		t.UpstreamPort = 9000
	}
	if t.IdleSeconds == 0 {
		t.IdleSeconds = 30
	}
	if t.IdleSeconds < 5 {
		http.Error(w, "idleSeconds must be at least 5", http.StatusBadRequest)
		return
	}
	saved, err := s.store.UpsertTenant(r.Context(), t)
	if err != nil {
		s.logger.Error("upsert tenant", "tenant", tenantID, "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	sleepy.WriteJSON(w, http.StatusOK, saved)
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	tenantID := strings.TrimPrefix(r.URL.Path, "/state/")
	if err := sleepy.ValidateTenantID(tenantID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	t, err := s.store.GetTenant(r.Context(), tenantID)
	if errors.Is(err, sleepy.ErrNotFound) {
		http.Error(w, "tenant not found", http.StatusNotFound)
		return
	}
	if err != nil {
		s.logger.Error("get tenant", "tenant", tenantID, "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	sleepy.WriteJSON(w, http.StatusOK, sleepy.ToStateResponse(t))
}

func (s *Server) handleWake(w http.ResponseWriter, r *http.Request) {
	tenantID := strings.TrimPrefix(r.URL.Path, "/wake/")
	if err := sleepy.ValidateTenantID(tenantID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	started := time.Now()
	s.logger.Info("wake requested", "tenant", tenantID)
	ctx, cancel := context.WithTimeout(r.Context(), s.wakeTimeout)
	defer cancel()

	t, err := s.wake(ctx, tenantID)
	if errors.Is(err, sleepy.ErrNotFound) {
		http.Error(w, "tenant not found", http.StatusNotFound)
		return
	}
	if err != nil {
		s.logger.Error("wake tenant", "tenant", tenantID, "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.logger.Info("wake complete", "tenant", tenantID, "state", t.State, "backend", t.Backend, "duration", time.Since(started).String())
	sleepy.WriteJSON(w, http.StatusOK, sleepy.ToStateResponse(t))
}

func (s *Server) wake(ctx context.Context, tenantID string) (sleepy.Tenant, error) {
	for {
		t, err := s.store.GetTenant(ctx, tenantID)
		if err != nil {
			return sleepy.Tenant{}, err
		}
		switch t.State {
		case sleepy.StateRunning:
			if t.Backend != "" {
				return t, nil
			}
			waking, ok, err := s.store.CompareAndSwapState(ctx, tenantID, t.Generation, sleepy.StateRunning, sleepy.StateWaking)
			if err != nil {
				return sleepy.Tenant{}, err
			}
			if !ok {
				continue
			}
			return s.createAndMarkRunning(ctx, waking)
		case sleepy.StateWaking:
			ready, err := s.waitForRunning(ctx, tenantID)
			if err != nil {
				return sleepy.Tenant{}, err
			}
			return ready, nil
		case sleepy.StateCold, sleepy.StateFailed:
			waking, ok, err := s.store.CompareAndSwapState(ctx, tenantID, t.Generation, t.State, sleepy.StateWaking)
			if err != nil {
				return sleepy.Tenant{}, err
			}
			if !ok {
				continue
			}
			return s.createAndMarkRunning(ctx, waking)
		case sleepy.StateDraining:
			if err := s.waitForStateChange(ctx, tenantID, sleepy.StateDraining); err != nil {
				return sleepy.Tenant{}, err
			}
			continue
		default:
			return s.waitForRunning(ctx, tenantID)
		}
	}
}

func (s *Server) createAndMarkRunning(ctx context.Context, t sleepy.Tenant) (sleepy.Tenant, error) {
	started := time.Now()
	s.logger.Info("tenant workload creating", "tenant", t.TenantID, "image", t.Image, "generation", t.Generation)
	backend, err := s.kube.EnsureTenant(ctx, t)
	if err != nil {
		_, _ = s.store.MarkFailed(ctx, t.TenantID, t.Generation, err.Error())
		return sleepy.Tenant{}, err
	}
	if err := s.kube.WaitReady(ctx, t.TenantID, s.wakeTimeout); err != nil {
		_, _ = s.store.MarkFailed(ctx, t.TenantID, t.Generation, err.Error())
		return sleepy.Tenant{}, err
	}
	running, err := s.store.MarkRunning(ctx, t.TenantID, t.Generation, backend)
	if err != nil {
		return sleepy.Tenant{}, err
	}
	s.logger.Info("tenant workload running", "tenant", t.TenantID, "backend", backend, "duration", time.Since(started).String())
	return running, nil
}

func (s *Server) waitForRunning(ctx context.Context, tenantID string) (sleepy.Tenant, error) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		t, err := s.store.GetTenant(ctx, tenantID)
		if err != nil {
			return sleepy.Tenant{}, err
		}
		if t.State == sleepy.StateRunning && t.Backend != "" {
			return t, nil
		}
		if t.State == sleepy.StateFailed {
			return t, errors.New(t.FailureReason)
		}
		select {
		case <-ctx.Done():
			return sleepy.Tenant{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Server) waitForStateChange(ctx context.Context, tenantID, state string) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		t, err := s.store.GetTenant(ctx, tenantID)
		if err != nil {
			return err
		}
		if t.State != state {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Server) handleSleep(w http.ResponseWriter, r *http.Request) {
	tenantID := strings.TrimPrefix(r.URL.Path, "/sleep/")
	if err := sleepy.ValidateTenantID(tenantID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var req sleepy.SleepRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.TenantID != "" && req.TenantID != tenantID {
		http.Error(w, "tenant ID mismatch", http.StatusBadRequest)
		return
	}
	t, err := s.store.GetTenant(r.Context(), tenantID)
	if errors.Is(err, sleepy.ErrNotFound) {
		http.Error(w, "tenant not found", http.StatusNotFound)
		return
	}
	if err != nil {
		s.logger.Error("get tenant before sleep", "tenant", tenantID, "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if t.State != sleepy.StateRunning {
		sleepy.WriteJSON(w, http.StatusOK, sleepy.ToStateResponse(t))
		return
	}
	started := time.Now()
	s.logger.Info("sleep requested", "tenant", tenantID, "reason", req.Reason, "activeConnections", req.ActiveConnections)
	draining, ok, err := s.store.CompareAndSwapState(r.Context(), tenantID, t.Generation, sleepy.StateRunning, sleepy.StateDraining)
	if err != nil {
		s.logger.Error("mark draining", "tenant", tenantID, "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !ok {
		sleepy.WriteJSON(w, http.StatusOK, sleepy.ToStateResponse(draining))
		return
	}
	if err := s.kube.DeleteTenant(r.Context(), tenantID); err != nil {
		_, _ = s.store.MarkFailed(r.Context(), tenantID, draining.Generation, err.Error())
		s.logger.Error("delete tenant workload", "tenant", tenantID, "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	cold, err := s.store.MarkCold(r.Context(), tenantID, draining.Generation)
	if err != nil {
		s.logger.Error("mark cold", "tenant", tenantID, "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.logger.Info("sleep complete", "tenant", tenantID, "duration", time.Since(started).String())
	sleepy.WriteJSON(w, http.StatusOK, sleepy.ToStateResponse(cold))
}
