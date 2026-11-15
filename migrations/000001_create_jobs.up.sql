CREATE TABLE jobs (
    id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        text        NOT NULL,
    queue            text        NOT NULL DEFAULT 'default',
    type             text        NOT NULL,                  -- handler registry key
    payload          jsonb       NOT NULL DEFAULT '{}',
    status           text        NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending','running','succeeded','failed','retrying','dead')),
    priority         smallint    NOT NULL DEFAULT 0,        -- higher runs first
    run_at           timestamptz NOT NULL DEFAULT now(),
    attempts         integer     NOT NULL DEFAULT 0,
    max_attempts     integer     NOT NULL DEFAULT 5 CHECK (max_attempts >= 1),
    locked_by        text,
    locked_until     timestamptz,
    idempotency_key  text,
    last_error       text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz,
    -- A running job must hold a lease; a job in any other state must not.
    CONSTRAINT lease_iff_running CHECK (
        (status = 'running' AND locked_by IS NOT NULL AND locked_until IS NOT NULL)
        OR (status <> 'running' AND locked_by IS NULL AND locked_until IS NULL)
    ),
    -- Idempotency is scoped per tenant. NULL keys never collide in a UNIQUE constraint.
    CONSTRAINT jobs_idempotency_uq UNIQUE (tenant_id, idempotency_key)
);

-- Hot path: claim. Partial, so it stays small however many finished rows exist.
CREATE INDEX jobs_claim_idx ON jobs (queue, priority DESC, run_at, id)
    WHERE status IN ('pending', 'retrying');

-- Reaper: find expired leases.
CREATE INDEX jobs_lease_idx ON jobs (locked_until) WHERE status = 'running';

-- Retention reaper.
CREATE INDEX jobs_finished_idx ON jobs (finished_at)
    WHERE status IN ('succeeded', 'failed', 'dead');

CREATE TABLE api_keys (
    key_hash    bytea       PRIMARY KEY,                    -- sha256(key); keys are 256-bit random
    tenant_id   text        NOT NULL,
    name        text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    revoked_at  timestamptz
);
