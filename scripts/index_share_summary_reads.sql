-- Cover long-range miner history without scanning other miners' summaries.
SET maintenance_work_mem = '128MB';
SET max_parallel_maintenance_workers = 0;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_share_read_miners_address_time
    ON share_read_miners(miner,minute) INCLUDE (status,shares,work);
