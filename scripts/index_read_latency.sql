-- Apply before deploying the API query changes to an existing pool database.
-- Run with psql -v ON_ERROR_STOP=1, outside a transaction.
-- Concurrent builds preserve share ingestion and payout writes.
SET maintenance_work_mem = '128MB';
SET max_parallel_maintenance_workers = 0;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_blocks_hash ON blocks(hash);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_blocks_timestamp_height
    ON blocks(timestamp DESC, height DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_payouts_visible_batch_timestamp
    ON payouts(batch_id, timestamp DESC) WHERE reverted_at IS NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_shares_accepted_time_difficulty
    ON shares(created_at) INCLUDE (difficulty)
    WHERE status IN ('verified', 'provisional');
