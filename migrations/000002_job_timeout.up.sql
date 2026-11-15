-- Per-job execution timeout (Phase 3). NULL means "use the worker default".
ALTER TABLE jobs ADD COLUMN timeout_seconds integer CHECK (timeout_seconds > 0);
