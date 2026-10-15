-- Run with psql -v ON_ERROR_STOP=1 -f scripts/backfill_share_read_minutes.sql.
-- Install capture first, then build both projections in one consistent snapshot.
SET lock_timeout = '2s';
SET work_mem = '64MB';
SELECT format('SET ROLE %I', pg_get_userbyid(relowner))
FROM pg_class WHERE oid='shares'::regclass \gexec
BEGIN;
\ir ../crates/pool-runtime/sql/share_read_minutes.sql
COMMIT;
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT ready FROM share_read_state WHERE singleton FOR UPDATE \gset
\if :ready
\echo 'Share minute projections are already initialized.'
\else
INSERT INTO share_read_miners(minute,miner,status,shares,work)
SELECT created_at / 60 * 60,miner,
       CASE WHEN status IN ('','verified','provisional') THEN status ELSE 'rejected' END,
       COUNT(*),SUM(difficulty)::bigint
FROM shares GROUP BY 1,2,3;
INSERT INTO share_read_minutes(minute,accepted,rejected,work)
SELECT minute,
       COALESCE(SUM(shares) FILTER (WHERE status IN ('verified','provisional')),0)::bigint,
       COALESCE(SUM(shares) FILTER (WHERE status NOT IN ('verified','provisional')),0)::bigint,
       COALESCE(SUM(work) FILTER (WHERE status IN ('verified','provisional')),0)::bigint
FROM share_read_miners GROUP BY minute;
-- The same snapshot excludes any writes committed after the backfill began.
DELETE FROM share_read_deltas;
UPDATE share_read_state SET ready=TRUE WHERE singleton;
\endif
COMMIT;
