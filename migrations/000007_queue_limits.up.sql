-- Per-tenant (or queue-wide) limits: concurrency cap, dispatch rate, and submit backpressure.
-- tenant_id = '*' is the default rule for every tenant on that queue that has no rule of its own.
CREATE TABLE queue_limits (
    queue              text             NOT NULL,
    tenant_id          text             NOT NULL,
    -- Concurrency cap: at most this many jobs of the scope may be `running` at once.
    max_running        integer          CHECK (max_running >= 1),
    -- Dispatch rate (token bucket): sustained jobs/second and burst size.
    rate_per_second    double precision CHECK (rate_per_second > 0),
    burst              integer          CHECK (burst >= 1),
    -- Backpressure: the API answers 429 when pending+retrying jobs in scope reach this.
    max_backlog        integer          CHECK (max_backlog >= 1),
    -- Token bucket state. Only touched under the row lock held during a claim.
    tokens             double precision NOT NULL DEFAULT 0,
    tokens_updated_at  timestamptz      NOT NULL DEFAULT now(),
    created_at         timestamptz      NOT NULL DEFAULT now(),
    updated_at         timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (queue, tenant_id),
    CONSTRAINT rate_needs_burst CHECK ((rate_per_second IS NULL) = (burst IS NULL)),
    CONSTRAINT limit_is_not_empty CHECK (max_running IS NOT NULL OR rate_per_second IS NOT NULL OR max_backlog IS NOT NULL)
);

-- Counting a scope's running jobs must not scan the table.
CREATE INDEX jobs_running_tenant_idx ON jobs (queue, tenant_id) WHERE status = 'running';
