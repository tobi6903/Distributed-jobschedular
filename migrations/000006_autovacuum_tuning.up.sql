-- The jobs table is high-churn: every job is inserted, updated at claim, and updated again at
-- completion (plus heartbeats), so dead tuples accumulate quickly. The defaults wait until 20%
-- of the table is dead, which on a large table means vacuum starts too late and the claim
-- index scans through dead entries. Vacuum earlier and let it work harder.
ALTER TABLE jobs SET (
    autovacuum_vacuum_scale_factor  = 0.02,
    autovacuum_vacuum_threshold     = 1000,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_vacuum_cost_limit    = 2000
);
