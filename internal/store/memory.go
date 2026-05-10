package store

import (
	"context"
	"sync"
	"time"

	"github.com/dangoodman/k8s-sleepy-proxy/internal/sleepy"
)

type MemoryStore struct {
	mu      sync.Mutex
	tenants map[string]sleepy.Tenant
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{tenants: map[string]sleepy.Tenant{}}
}

func (s *MemoryStore) EnsureSchema(context.Context) error {
	return nil
}

func (s *MemoryStore) GetTenant(_ context.Context, id string) (sleepy.Tenant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tenants[id]
	if !ok {
		return sleepy.Tenant{}, sleepy.ErrNotFound
	}
	return t, nil
}

func (s *MemoryStore) UpsertTenant(_ context.Context, t sleepy.Tenant) (sleepy.Tenant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t = sleepy.ApplyTenantDefaults(t)
	if old, ok := s.tenants[t.TenantID]; ok {
		t.State = old.State
		t.Backend = old.Backend
		t.FailureReason = old.FailureReason
		t.Generation = old.Generation + 1
		if t.LastActiveAt.IsZero() {
			t.LastActiveAt = old.LastActiveAt
		}
	} else {
		t.State = sleepy.StateCold
		t.Generation = 1
		if t.LastActiveAt.IsZero() {
			t.LastActiveAt = time.Now().UTC()
		}
	}
	s.tenants[t.TenantID] = t
	return t, nil
}

func (s *MemoryStore) CompareAndSwapState(_ context.Context, id string, generation int64, fromState, toState string) (sleepy.Tenant, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tenants[id]
	if !ok {
		return sleepy.Tenant{}, false, sleepy.ErrNotFound
	}
	if t.Generation != generation || t.State != fromState {
		return t, false, nil
	}
	t.State = toState
	t.Generation++
	t.FailureReason = ""
	s.tenants[id] = t
	return t, true, nil
}

func (s *MemoryStore) MarkRunning(_ context.Context, id string, generation int64, backend string) (sleepy.Tenant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tenants[id]
	if !ok {
		return sleepy.Tenant{}, sleepy.ErrNotFound
	}
	if t.Generation == generation && t.State == sleepy.StateWaking {
		t.State = sleepy.StateRunning
		t.Backend = backend
		t.FailureReason = ""
		t.Generation++
		s.tenants[id] = t
	}
	return t, nil
}

func (s *MemoryStore) MarkFailed(_ context.Context, id string, generation int64, reason string) (sleepy.Tenant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tenants[id]
	if !ok {
		return sleepy.Tenant{}, sleepy.ErrNotFound
	}
	if t.Generation == generation {
		t.State = sleepy.StateFailed
		t.Backend = ""
		t.FailureReason = reason
		t.Generation++
		s.tenants[id] = t
	}
	return t, nil
}

func (s *MemoryStore) MarkCold(_ context.Context, id string, generation int64) (sleepy.Tenant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tenants[id]
	if !ok {
		return sleepy.Tenant{}, sleepy.ErrNotFound
	}
	if t.Generation == generation {
		t.State = sleepy.StateCold
		t.Backend = ""
		t.FailureReason = ""
		t.Generation++
		s.tenants[id] = t
	}
	return t, nil
}

func (s *MemoryStore) TouchTenant(_ context.Context, id string, lastActiveAt time.Time) (sleepy.Tenant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tenants[id]
	if !ok {
		return sleepy.Tenant{}, sleepy.ErrNotFound
	}
	t.LastActiveAt = lastActiveAt
	s.tenants[id] = t
	return t, nil
}
