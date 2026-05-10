package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/dangoodman/k8s-sleepy-proxy/internal/sleepy"
)

type cacheEntry struct {
	state   sleepy.StateResponse
	expires time.Time
}

type server struct {
	controllerURL string
	authToken     string
	client        *http.Client
	cacheTTL      time.Duration
	wakeTimeout   time.Duration
	mu            sync.Mutex
	cache         map[string]cacheEntry
	logger        *slog.Logger
}

func main() {
	cacheWarm := flag.Bool("cache-warm", false, "exit immediately after image pull")
	flag.Parse()
	if *cacheWarm {
		return
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	s := &server{
		controllerURL: sleepy.Env("CONTROLLER_URL", "http://sleepy-controller.sleepy-system.svc.cluster.local:8080"),
		authToken:     sleepy.MustEnv("AUTH_TOKEN"),
		client:        &http.Client{Timeout: 130 * time.Second},
		cacheTTL:      sleepy.EnvDurationSeconds("CACHE_TTL_SECONDS", 5*time.Second),
		wakeTimeout:   sleepy.EnvDurationSeconds("WAKE_TIMEOUT_SECONDS", 120*time.Second),
		cache:         map[string]cacheEntry{},
		logger:        logger,
	}
	listenAddr := sleepy.Env("LISTEN_ADDR", ":8080")
	logger.Info("lb listening", "addr", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, s))
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	tenantID := r.Header.Get("TENANT")
	if tenantID == "" {
		http.Error(w, "TENANT header is required", http.StatusBadRequest)
		return
	}
	if err := sleepy.ValidateTenantID(tenantID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	started := time.Now()
	state, source, err := s.resolveBackend(r.Context(), tenantID)
	if err != nil {
		status := http.StatusBadGateway
		if errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusGatewayTimeout
		}
		s.logger.Error("resolve backend", "tenant", tenantID, "err", err)
		http.Error(w, err.Error(), status)
		return
	}
	if state.State != sleepy.StateRunning || state.Backend == "" {
		http.Error(w, "tenant unavailable", http.StatusServiceUnavailable)
		return
	}
	s.logInfo("lb route", "tenant", tenantID, "source", source, "backend", state.Backend, "method", r.Method, "path", r.URL.Path, "resolveDuration", time.Since(started).String())
	target, _ := url.Parse("http://" + state.Backend)
	proxy := httputil.NewSingleHostReverseProxy(target)
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = target.Host
		req.Header.Set("X-Sleepy-Tenant", tenantID)
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		s.evict(tenantID)
		s.logger.Error("proxy request", "tenant", tenantID, "backend", state.Backend, "err", err)
		http.Error(w, err.Error(), http.StatusBadGateway)
	}
	proxy.ServeHTTP(w, r)
}

func (s *server) resolveBackend(ctx context.Context, tenantID string) (sleepy.StateResponse, string, error) {
	if cached, ok := s.cached(tenantID); ok && cached.State == sleepy.StateRunning && cached.Backend != "" {
		return cached, "memory_cache", nil
	}
	state, err := s.state(ctx, tenantID)
	if err != nil {
		return sleepy.StateResponse{}, "", err
	}
	switch state.State {
	case sleepy.StateRunning:
		s.putCache(state)
		return state, "controller_state", nil
	case sleepy.StateCold, sleepy.StateFailed:
		ctx, cancel := context.WithTimeout(ctx, s.wakeTimeout)
		defer cancel()
		state, err = s.wake(ctx, tenantID)
		if err != nil {
			return sleepy.StateResponse{}, "", err
		}
		if state.State != sleepy.StateRunning || state.Backend == "" {
			return state, "", fmt.Errorf("wake returned %s: %s", state.State, state.FailureReason)
		}
		s.putCache(state)
		return state, "controller_wake", nil
	case sleepy.StateWaking, sleepy.StateDraining:
		state, err := s.pollRunning(ctx, tenantID)
		if err != nil {
			return sleepy.StateResponse{}, "", err
		}
		return state, "controller_poll", nil
	default:
		return sleepy.StateResponse{}, "", fmt.Errorf("unknown tenant state %q", state.State)
	}
}

func (s *server) pollRunning(ctx context.Context, tenantID string) (sleepy.StateResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.wakeTimeout)
	defer cancel()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		state, err := s.state(ctx, tenantID)
		if err != nil {
			return sleepy.StateResponse{}, err
		}
		if state.State == sleepy.StateRunning && state.Backend != "" {
			s.putCache(state)
			return state, nil
		}
		if state.State == sleepy.StateFailed {
			return state, fmt.Errorf("tenant failed: %s", state.FailureReason)
		}
		select {
		case <-ctx.Done():
			return sleepy.StateResponse{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *server) state(ctx context.Context, tenantID string) (sleepy.StateResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.controllerURL+"/state/"+tenantID, nil)
	if err != nil {
		return sleepy.StateResponse{}, err
	}
	req.Header.Set("Authorization", "Bearer "+s.authToken)
	return s.doState(req)
}

func (s *server) wake(ctx context.Context, tenantID string) (sleepy.StateResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.controllerURL+"/wake/"+tenantID, bytes.NewReader(nil))
	if err != nil {
		return sleepy.StateResponse{}, err
	}
	req.Header.Set("Authorization", "Bearer "+s.authToken)
	return s.doState(req)
}

func (s *server) doState(req *http.Request) (sleepy.StateResponse, error) {
	resp, err := s.client.Do(req)
	if err != nil {
		return sleepy.StateResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return sleepy.StateResponse{}, fmt.Errorf("%s returned %s", req.URL.Path, resp.Status)
	}
	var state sleepy.StateResponse
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		return sleepy.StateResponse{}, err
	}
	return state, nil
}

func (s *server) cached(tenantID string) (sleepy.StateResponse, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.cache[tenantID]
	if !ok || time.Now().After(entry.expires) {
		delete(s.cache, tenantID)
		return sleepy.StateResponse{}, false
	}
	return entry.state, true
}

func (s *server) putCache(state sleepy.StateResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache[state.TenantID] = cacheEntry{state: state, expires: time.Now().Add(s.cacheTTL)}
}

func (s *server) evict(tenantID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.cache, tenantID)
}

func (s *server) logInfo(msg string, args ...any) {
	if s.logger != nil {
		s.logger.Info(msg, args...)
	}
}
