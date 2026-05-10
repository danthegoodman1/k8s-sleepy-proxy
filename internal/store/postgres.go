package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/dangoodman/k8s-sleepy-proxy/internal/sleepy"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(databaseURL string) (*PostgresStore, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(30 * time.Minute)
	return &PostgresStore{db: db}, nil
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}

func (s *PostgresStore) EnsureSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS tenants (
  tenant_id text PRIMARY KEY,
  image text NOT NULL,
  upstream_port integer NOT NULL,
  public_host text NOT NULL DEFAULT '',
  idle_seconds integer NOT NULL,
  state text NOT NULL,
  last_active_at timestamptz NOT NULL,
  generation bigint NOT NULL,
  backend text,
  failure_reason text
);

CREATE INDEX IF NOT EXISTS tenants_state_idx ON tenants(state);
`)
	return err
}

func (s *PostgresStore) GetTenant(ctx context.Context, id string) (sleepy.Tenant, error) {
	row := s.db.QueryRowContext(ctx, tenantSelectSQL()+` WHERE tenant_id = $1`, id)
	return scanTenant(row)
}

func (s *PostgresStore) UpsertTenant(ctx context.Context, t sleepy.Tenant) (sleepy.Tenant, error) {
	if t.LastActiveAt.IsZero() {
		t.LastActiveAt = time.Now().UTC()
	}
	row := s.db.QueryRowContext(ctx, `
INSERT INTO tenants (
  tenant_id, image, upstream_port, public_host, idle_seconds, state, last_active_at, generation
) VALUES (
  $1, $2, $3, $4, $5, 'Cold', $6, 1
)
ON CONFLICT (tenant_id) DO UPDATE SET
  image = EXCLUDED.image,
  upstream_port = EXCLUDED.upstream_port,
  public_host = EXCLUDED.public_host,
  idle_seconds = EXCLUDED.idle_seconds,
  generation = tenants.generation + 1
RETURNING tenant_id, image, upstream_port, public_host, idle_seconds, state,
  last_active_at, generation, backend, failure_reason
`, t.TenantID, t.Image, t.UpstreamPort, t.PublicHost, t.IdleSeconds, t.LastActiveAt)
	return scanTenant(row)
}

func (s *PostgresStore) CompareAndSwapState(ctx context.Context, id string, generation int64, fromState, toState string) (sleepy.Tenant, bool, error) {
	row := s.db.QueryRowContext(ctx, `
UPDATE tenants
SET state = $4, generation = generation + 1, failure_reason = NULL
WHERE tenant_id = $1 AND generation = $2 AND state = $3
RETURNING tenant_id, image, upstream_port, public_host, idle_seconds, state,
  last_active_at, generation, backend, failure_reason
`, id, generation, fromState, toState)
	t, err := scanTenant(row)
	if err == nil {
		return t, true, nil
	}
	if errors.Is(err, sleepy.ErrNotFound) {
		current, getErr := s.GetTenant(ctx, id)
		if getErr != nil {
			return sleepy.Tenant{}, false, getErr
		}
		return current, false, nil
	}
	return sleepy.Tenant{}, false, err
}

func (s *PostgresStore) MarkRunning(ctx context.Context, id string, generation int64, backend string) (sleepy.Tenant, error) {
	row := s.db.QueryRowContext(ctx, `
UPDATE tenants
SET state = 'Running', backend = $3, failure_reason = NULL, generation = generation + 1, last_active_at = now()
WHERE tenant_id = $1 AND generation = $2 AND state = 'Waking'
RETURNING tenant_id, image, upstream_port, public_host, idle_seconds, state,
  last_active_at, generation, backend, failure_reason
`, id, generation, backend)
	t, err := scanTenant(row)
	if errors.Is(err, sleepy.ErrNotFound) {
		return s.GetTenant(ctx, id)
	}
	return t, err
}

func (s *PostgresStore) MarkFailed(ctx context.Context, id string, generation int64, reason string) (sleepy.Tenant, error) {
	row := s.db.QueryRowContext(ctx, `
UPDATE tenants
SET state = 'Failed', backend = NULL, failure_reason = $3, generation = generation + 1
WHERE tenant_id = $1 AND generation = $2
RETURNING tenant_id, image, upstream_port, public_host, idle_seconds, state,
  last_active_at, generation, backend, failure_reason
`, id, generation, reason)
	t, err := scanTenant(row)
	if errors.Is(err, sleepy.ErrNotFound) {
		return s.GetTenant(ctx, id)
	}
	return t, err
}

func (s *PostgresStore) MarkCold(ctx context.Context, id string, generation int64) (sleepy.Tenant, error) {
	row := s.db.QueryRowContext(ctx, `
UPDATE tenants
SET state = 'Cold', backend = NULL, failure_reason = NULL, generation = generation + 1
WHERE tenant_id = $1 AND generation = $2
RETURNING tenant_id, image, upstream_port, public_host, idle_seconds, state,
  last_active_at, generation, backend, failure_reason
`, id, generation)
	t, err := scanTenant(row)
	if errors.Is(err, sleepy.ErrNotFound) {
		return s.GetTenant(ctx, id)
	}
	return t, err
}

func (s *PostgresStore) TouchTenant(ctx context.Context, id string, lastActiveAt time.Time) (sleepy.Tenant, error) {
	row := s.db.QueryRowContext(ctx, `
UPDATE tenants
SET last_active_at = $2
WHERE tenant_id = $1
RETURNING tenant_id, image, upstream_port, public_host, idle_seconds, state,
  last_active_at, generation, backend, failure_reason
`, id, lastActiveAt)
	return scanTenant(row)
}

type scanner interface {
	Scan(dest ...any) error
}

func tenantSelectSQL() string {
	return `SELECT tenant_id, image, upstream_port, public_host, idle_seconds, state,
  last_active_at, generation, backend, failure_reason FROM tenants`
}

func scanTenant(row scanner) (sleepy.Tenant, error) {
	var t sleepy.Tenant
	var backend, failure sql.NullString
	err := row.Scan(
		&t.TenantID,
		&t.Image,
		&t.UpstreamPort,
		&t.PublicHost,
		&t.IdleSeconds,
		&t.State,
		&t.LastActiveAt,
		&t.Generation,
		&backend,
		&failure,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return sleepy.Tenant{}, sleepy.ErrNotFound
	}
	if err != nil {
		return sleepy.Tenant{}, err
	}
	if backend.Valid {
		t.Backend = backend.String
	}
	if failure.Valid {
		t.FailureReason = failure.String
	}
	return t, nil
}
