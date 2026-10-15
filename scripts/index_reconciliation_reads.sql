-- Sparse reconciliation queues: avoid scanning historical paid-out rows.
-- These indexes can be installed independently of an API/daemon release.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_payouts_unreconciled
    ON payouts(timestamp DESC,id DESC) INCLUDE(address,amount,fee,tx_hash)
    WHERE reverted_at IS NULL AND reversible=FALSE AND COALESCE(tx_hash,'') <> '';
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_blocks_orphaned_height
    ON blocks(height DESC,hash DESC) WHERE orphaned=TRUE;
