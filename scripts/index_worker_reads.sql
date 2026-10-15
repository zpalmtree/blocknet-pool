-- Run before deploying to an existing database. CONCURRENTLY preserves writes.
SET maintenance_work_mem = '128MB';
SET max_parallel_maintenance_workers = 0;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_shares_miner_read_window
    ON shares(miner, created_at DESC)
    INCLUDE (worker, status, difficulty, miner_version, backend);
