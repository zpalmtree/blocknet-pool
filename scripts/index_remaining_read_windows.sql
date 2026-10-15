-- Apply before deploying the API. All builds leave normal reads/writes online.
SET maintenance_work_mem = '128MB';
SET max_parallel_maintenance_workers = 0;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_credit_unpaid
    ON block_credit_events(block_height,address) INCLUDE(amount,paid_amount)
    WHERE amount > paid_amount;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pool_fee_credit_unpaid
    ON pool_fee_balance_credits(block_height,fee_address) INCLUDE(amount,paid_amount)
    WHERE amount > paid_amount;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_shares_worker_window
    ON shares(created_at) INCLUDE(miner,worker,status,difficulty);
-- The new index covers every column used by the previous preview index.
DROP INDEX CONCURRENTLY IF EXISTS idx_shares_preview_time;
