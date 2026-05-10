package sleepy

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"
)

const (
	StateCold     = "Cold"
	StateWaking   = "Waking"
	StateRunning  = "Running"
	StateDraining = "Draining"
	StateFailed   = "Failed"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrInvalidTenant = errors.New("invalid tenant id")

	tenantIDPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,54}[a-z0-9])?$`)
)

type Tenant struct {
	TenantID      string    `json:"tenantId"`
	Image         string    `json:"image"`
	UpstreamPort  int       `json:"upstreamPort"`
	PublicHost    string    `json:"publicHost"`
	IdleSeconds   int       `json:"idleSeconds"`
	State         string    `json:"state"`
	LastActiveAt  time.Time `json:"lastActiveAt"`
	Generation    int64     `json:"generation"`
	Backend       string    `json:"backend,omitempty"`
	FailureReason string    `json:"failureReason,omitempty"`
}

type TenantStore interface {
	EnsureSchema(context.Context) error
	GetTenant(context.Context, string) (Tenant, error)
	UpsertTenant(context.Context, Tenant) (Tenant, error)
	CompareAndSwapState(context.Context, string, int64, string, string) (Tenant, bool, error)
	MarkRunning(context.Context, string, int64, string) (Tenant, error)
	MarkFailed(context.Context, string, int64, string) (Tenant, error)
	MarkCold(context.Context, string, int64) (Tenant, error)
	TouchTenant(context.Context, string, time.Time) (Tenant, error)
}

type StateResponse struct {
	TenantID      string `json:"tenantId"`
	State         string `json:"state"`
	Backend       string `json:"backend,omitempty"`
	Generation    int64  `json:"generation"`
	FailureReason string `json:"failureReason,omitempty"`
}

type SleepRequest struct {
	TenantID          string    `json:"tenantId"`
	PodName           string    `json:"podName"`
	ActiveConnections int       `json:"activeConnections"`
	LastTrafficAt     time.Time `json:"lastTrafficAt"`
	Reason            string    `json:"reason"`
}

func ValidateTenantID(id string) error {
	if !tenantIDPattern.MatchString(id) {
		return fmt.Errorf("%w: %q must be lowercase DNS-label compatible and at most 56 chars", ErrInvalidTenant, id)
	}
	return nil
}

func WorkloadName(tenantID string) string {
	return "sleepy-" + tenantID
}

func TenantLabels(tenantID string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name": "sleepy-tenant",
		"sleepy.dev/managed-by":  "sleepy-controller",
		"sleepy.dev/tenant-id":   tenantID,
	}
}

func ToStateResponse(t Tenant) StateResponse {
	return StateResponse{
		TenantID:      t.TenantID,
		State:         t.State,
		Backend:       t.Backend,
		Generation:    t.Generation,
		FailureReason: t.FailureReason,
	}
}
