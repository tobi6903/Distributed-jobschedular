-- W3C traceparent captured at submit time (or at cron fire time) so the worker's execute
-- span joins the same trace as whatever created the job (Phase 5).
ALTER TABLE jobs ADD COLUMN traceparent text;
