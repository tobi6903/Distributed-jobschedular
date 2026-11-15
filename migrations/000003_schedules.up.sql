-- Cron schedules (Phase 4). The scheduler turns a due schedule into a job row.
CREATE TABLE schedules (
    id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        text        NOT NULL,
    name             text        NOT NULL,
    cron_expr        text        NOT NULL,
    timezone         text        NOT NULL DEFAULT 'UTC',   -- IANA name, e.g. America/New_York
    misfire_policy   text        NOT NULL DEFAULT 'fire_once'
                     CHECK (misfire_policy IN ('fire_once', 'skip', 'fire_all')),
    enabled          boolean     NOT NULL DEFAULT true,
    -- Job template
    queue            text        NOT NULL DEFAULT 'default',
    type             text        NOT NULL,
    payload          jsonb       NOT NULL DEFAULT '{}',
    priority         smallint    NOT NULL DEFAULT 0,
    max_attempts     integer     NOT NULL DEFAULT 5 CHECK (max_attempts >= 1),
    timeout_seconds  integer     CHECK (timeout_seconds > 0),
    -- Scheduler state
    next_fire_at     timestamptz NOT NULL,
    last_fired_at    timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT schedules_name_uq UNIQUE (tenant_id, name)
);

CREATE INDEX schedules_due_idx ON schedules (next_fire_at) WHERE enabled;
