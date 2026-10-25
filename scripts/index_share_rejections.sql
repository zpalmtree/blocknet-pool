-- Apply online without restarting the pool or blocking share ingestion:
-- ssh bntpool 'sudo -u postgres psql -X -v ON_ERROR_STOP=1 -d blocknet_pool' < scripts/index_share_rejections.sql
-- Run outside a transaction. A failed concurrent build can leave an invalid
-- index: inspect pg_index.indisvalid and drop that invalid index concurrently
-- before retrying (IF NOT EXISTS does not repair an invalid index).
\set ON_ERROR_STOP on
SET lock_timeout = '10s';
SET statement_timeout = '15min';
-- Both windowed and lifetime rejection counts can read only rejected shares.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_shares_rejected_created_reason
    ON public.shares (created_at DESC) INCLUDE (reject_reason)
    WHERE status NOT IN ('verified', 'provisional');

SELECT indexrelid::regclass AS index_name, indisvalid, indisready,
       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_index
WHERE indexrelid = 'public.idx_shares_rejected_created_reason'::regclass;
