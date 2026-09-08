# Independent review: lease claim passes an uncommitted effect barrier

Reproduced on 2026-09-07 using an isolated `postgres:17-alpine` container,
removed immediately after the probe. This uses a minimal schema and the same
row-lock / `NOT EXISTS` query ordering as the initial Phase 6B/D implementation.
It demonstrates PostgreSQL snapshot behavior; it is not a full API test.

Setup:

```sql
CREATE TABLE materializations (
    materialization_id text PRIMARY KEY,
    reconcile_owner text,
    reconcile_attempt bigint,
    reconcile_lease_expires_at_unix_millis bigint,
    state text,
    next_attempt_at_unix_millis bigint,
    drain_not_before_unix_millis bigint,
    updated_at_unix_millis bigint
);
CREATE TABLE materialization_effects (
    materialization_id text PRIMARY KEY REFERENCES materializations
);
INSERT INTO materializations VALUES (
    'mat', 'old', 1,
    (extract(epoch from clock_timestamp()) * 1000)::bigint + 800,
    'pending', 0, 0, 0
);
```

Session A starts before expiry, locks the materialization and inserts a barrier:

```sql
BEGIN;
SELECT 1 FROM materializations
WHERE materialization_id = 'mat'
  AND reconcile_owner = 'old' AND reconcile_attempt = 1
  AND reconcile_lease_expires_at_unix_millis >
      (extract(epoch from clock_timestamp()) * 1000)::bigint
FOR UPDATE;
INSERT INTO materialization_effects VALUES ('mat');
SELECT pg_sleep(2);
COMMIT;
```

After approximately 1.1 seconds, session B records database time as `:now`
and runs this statement while A still holds the row lock:

```sql
UPDATE materializations
SET reconcile_owner = 'new',
    reconcile_attempt = reconcile_attempt + 1,
    reconcile_lease_expires_at_unix_millis = :now + 60000
WHERE materialization_id = 'mat'
  AND state IN ('pending', 'deleting')
  AND NOT EXISTS (
      SELECT 1 FROM materialization_effects e
      WHERE e.materialization_id = materializations.materialization_id
  )
  AND next_attempt_at_unix_millis <= :now
  AND drain_not_before_unix_millis <= :now
  AND (reconcile_owner IS NULL OR reconcile_lease_expires_at_unix_millis <= :now)
RETURNING reconcile_owner, reconcile_attempt;
```

B waits for A, then returns:

```text
new|2
UPDATE 1
```

The subsequent committed-state query:

```sql
SELECT reconcile_owner, reconcile_attempt,
       EXISTS (SELECT 1 FROM materialization_effects)
FROM materializations;
```

returns `new|2|t`: ownership transferred while the unresolved barrier remained.
The claim statement's snapshot predates A's barrier commit. A row lock taken
by A without updating the materialization does not refresh B's view of the
separate effect table. Completion and dispatch checks provide additional
protection, but the claimed quarantine invariant itself is false.

Required fix: claim in a transaction, acquire the materialization row lock,
then inspect the effect table in a separate statement with a fresh snapshot
before transferring ownership. Keep a deterministic real-store regression for
this ordering and verify that the returned claim is absent.
