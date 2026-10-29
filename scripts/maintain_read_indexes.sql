-- Keep recent append-heavy pages visible to index-only API reads.
-- Default scale factors allowed more than a million new shares between vacuums.
-- Apply with psql -v ON_ERROR_STOP=1, outside a transaction; no restart required.
SET lock_timeout = '2s';
-- A partitioned shares table takes storage parameters per partition; new
-- weekly partitions copy them from the newest one.
DO $shares$
DECLARE
    target regclass;
BEGIN
    FOR target IN
        SELECT 'shares'::regclass
        WHERE (SELECT relkind FROM pg_class WHERE oid = 'shares'::regclass) = 'r'
        UNION ALL
        SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent = 'shares'::regclass
    LOOP
        EXECUTE format('ALTER TABLE %s SET (
            autovacuum_vacuum_insert_threshold = 20000,
            autovacuum_vacuum_insert_scale_factor = 0,
            autovacuum_analyze_scale_factor = 0.01
        )', target);
    END LOOP;
END
$shares$;
ALTER TABLE payouts SET (
    autovacuum_vacuum_insert_threshold = 2000,
    autovacuum_vacuum_insert_scale_factor = 0
);

-- Ordinary online vacuum; avoid tail truncation's exclusive lock and cap I/O.
SET vacuum_cost_limit = 1000;
SET vacuum_cost_delay = '2ms';
SET maintenance_work_mem = '128MB';
VACUUM (ANALYZE, TRUNCATE FALSE, PARALLEL 0) shares;
VACUUM (ANALYZE, TRUNCATE FALSE, PARALLEL 0) payouts;
