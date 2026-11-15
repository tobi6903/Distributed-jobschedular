-- Cheap DLQ-size metric: count only dead rows instead of scanning all finished rows.
CREATE INDEX jobs_dead_idx ON jobs (queue) WHERE status = 'dead';
