-- Cover exact boundary reads used by share-window summaries.
SET maintenance_work_mem = '128MB';
SET max_parallel_maintenance_workers = 0;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_shares_preview_time
    ON shares(created_at) INCLUDE (miner,status,difficulty);
