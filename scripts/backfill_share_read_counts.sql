-- Run with psql -v ON_ERROR_STOP=1 -f scripts/backfill_share_read_counts.sql.
-- The short DDL transaction installs capture before the nonblocking scan.
-- Retry the whole script if lock_timeout or a serialization failure occurs.
SET lock_timeout = '2s';
-- Own the projection with the same role as shares so existing writer services
-- can execute capture immediately, even when migration is run by postgres.
SELECT format('SET ROLE %I', pg_get_userbyid(relowner))
FROM pg_class WHERE oid = 'shares'::regclass \gexec
BEGIN;
\ir ../crates/pool-runtime/sql/share_read_counts.sql
COMMIT;

BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT ready FROM miner_share_count_state WHERE singleton FOR UPDATE \gset
\if :ready
\echo 'Share read counts are already initialized.'
\else
INSERT INTO miner_share_counts(miner,accepted,rejected)
SELECT miner,
       COUNT(*) FILTER (WHERE status IN ('verified','provisional')),
       COUNT(*) FILTER (WHERE status NOT IN ('verified','provisional'))
FROM shares GROUP BY miner;
-- Delete precisely the deltas visible to the backfill snapshot. Transactions
-- committing later retain their deltas, including ones with lower sequence IDs.
DELETE FROM miner_share_count_deltas;
UPDATE miner_share_count_state SET ready = TRUE WHERE singleton;
\endif
COMMIT;
