-- Exact per-minute projections. Statement triggers aggregate bulk writes before
-- enqueueing; readers combine committed summaries and unapplied deltas.
CREATE TABLE IF NOT EXISTS share_read_miners (
    minute BIGINT NOT NULL, miner TEXT NOT NULL, status TEXT NOT NULL,
    shares BIGINT NOT NULL, work BIGINT NOT NULL,
    PRIMARY KEY (minute,miner,status)
);
CREATE TABLE IF NOT EXISTS share_read_minutes (
    minute BIGINT PRIMARY KEY,
    accepted BIGINT NOT NULL, rejected BIGINT NOT NULL, work BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS share_read_deltas (
    id BIGSERIAL PRIMARY KEY,
    minute BIGINT NOT NULL, miner TEXT NOT NULL, status TEXT NOT NULL,
    shares BIGINT NOT NULL, work BIGINT NOT NULL
) WITH (autovacuum_vacuum_threshold = 1000, autovacuum_vacuum_scale_factor = 0);
CREATE TABLE IF NOT EXISTS share_read_state (
    singleton BOOLEAN PRIMARY KEY CHECK (singleton), ready BOOLEAN NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_share_read_deltas_minute ON share_read_deltas(minute);
CREATE INDEX IF NOT EXISTS idx_share_read_miners_empty ON share_read_miners(minute,miner,status)
    WHERE shares = 0 AND work = 0;
CREATE INDEX IF NOT EXISTS idx_share_read_minutes_empty ON share_read_minutes(minute)
    WHERE accepted = 0 AND rejected = 0 AND work = 0;

CREATE OR REPLACE FUNCTION capture_share_read_minutes() RETURNS trigger
LANGUAGE plpgsql AS $function$
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        DELETE FROM share_read_deltas;
        DELETE FROM share_read_miners;
        DELETE FROM share_read_minutes;
        UPDATE share_read_state SET ready=TRUE WHERE singleton;
    ELSIF TG_OP = 'INSERT' THEN
        INSERT INTO share_read_deltas(minute,miner,status,shares,work)
        SELECT created_at / 60 * 60,miner,
               CASE WHEN status IN ('','verified','provisional') THEN status ELSE 'rejected' END,
               COUNT(*),SUM(difficulty)::bigint
        FROM new_share_rows GROUP BY 1,2,3;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO share_read_deltas(minute,miner,status,shares,work)
        SELECT created_at / 60 * 60,miner,
               CASE WHEN status IN ('','verified','provisional') THEN status ELSE 'rejected' END,
               -COUNT(*),-SUM(difficulty)::bigint
        FROM old_share_rows GROUP BY 1,2,3;
    ELSE
        INSERT INTO share_read_deltas(minute,miner,status,shares,work)
        SELECT minute,miner,status,SUM(n)::bigint,SUM(work)::bigint
        FROM (
            SELECT created_at / 60 * 60 AS minute,miner,
                   CASE WHEN status IN ('','verified','provisional') THEN status ELSE 'rejected' END AS status,
                   1 AS n,difficulty AS work FROM new_share_rows
            UNION ALL
            SELECT created_at / 60 * 60,miner,
                   CASE WHEN status IN ('','verified','provisional') THEN status ELSE 'rejected' END,
                   -1,-difficulty FROM old_share_rows
        ) changes GROUP BY minute,miner,status
        HAVING SUM(n) <> 0 OR SUM(work) <> 0;
    END IF;
    RETURN NULL;
END
$function$;

DO $install$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid='shares'::regclass
                   AND tgname='share_read_insert') THEN
        CREATE TRIGGER share_read_insert AFTER INSERT ON shares
        REFERENCING NEW TABLE AS new_share_rows FOR EACH STATEMENT
        EXECUTE FUNCTION capture_share_read_minutes();
        CREATE TRIGGER share_read_delete AFTER DELETE ON shares
        REFERENCING OLD TABLE AS old_share_rows FOR EACH STATEMENT
        EXECUTE FUNCTION capture_share_read_minutes();
        CREATE TRIGGER share_read_update AFTER UPDATE ON shares
        REFERENCING OLD TABLE AS old_share_rows NEW TABLE AS new_share_rows
        FOR EACH STATEMENT EXECUTE FUNCTION capture_share_read_minutes();
        CREATE TRIGGER share_read_truncate AFTER TRUNCATE ON shares
        FOR EACH STATEMENT EXECUTE FUNCTION capture_share_read_minutes();
    END IF;
END
$install$;
INSERT INTO share_read_state(singleton,ready)
VALUES (TRUE, NOT EXISTS (SELECT 1 FROM shares LIMIT 1))
ON CONFLICT (singleton) DO NOTHING;

-- STABLE keeps summary rows, unapplied deltas, and raw boundaries on the
-- caller's single MVCC snapshot, even while compaction or mining commits.
CREATE OR REPLACE FUNCTION share_read_window(p_start BIGINT,p_end BIGINT)
RETURNS TABLE(accepted BIGINT,rejected BIGINT,work BIGINT)
LANGUAGE plpgsql STABLE ROWS 1 AS $window$
DECLARE
    full_start BIGINT;
    full_end BIGINT;
BEGIN
    IF p_start < 0 OR p_start > 9223372036854775740
       OR NOT COALESCE((SELECT ready FROM share_read_state WHERE singleton),FALSE) THEN
        RETURN QUERY SELECT
            COUNT(*) FILTER (WHERE status IN ('verified','provisional')),
            COUNT(*) FILTER (WHERE status NOT IN ('verified','provisional')),
            COALESCE(SUM(difficulty) FILTER (WHERE status IN ('verified','provisional')),0)::bigint
        FROM shares WHERE created_at >= p_start AND created_at <= p_end;
        RETURN;
    END IF;
    full_start := p_start / 60 * 60 + CASE WHEN p_start % 60 > 0 THEN 60 ELSE 0 END;
    full_end := p_end / 60 * 60 + CASE WHEN p_end % 60 = 59 THEN 60 ELSE 0 END;
    IF full_start >= full_end THEN
        RETURN QUERY SELECT
            COUNT(*) FILTER (WHERE status IN ('verified','provisional')),
            COUNT(*) FILTER (WHERE status NOT IN ('verified','provisional')),
            COALESCE(SUM(difficulty) FILTER (WHERE status IN ('verified','provisional')),0)::bigint
        FROM shares WHERE created_at >= p_start AND created_at <= p_end;
        RETURN;
    END IF;
    RETURN QUERY SELECT COALESCE(SUM(x.a),0)::bigint,COALESCE(SUM(x.r),0)::bigint,COALESCE(SUM(x.w),0)::bigint
    FROM (
        SELECT m.accepted AS a,m.rejected AS r,m.work AS w FROM share_read_minutes m
        WHERE minute >= full_start AND minute < full_end
        UNION ALL
        SELECT CASE WHEN status IN ('verified','provisional') THEN d.shares ELSE 0 END,
               CASE WHEN status NOT IN ('verified','provisional') THEN d.shares ELSE 0 END,
               CASE WHEN status IN ('verified','provisional') THEN d.work ELSE 0 END
        FROM share_read_deltas d WHERE minute >= full_start AND minute < full_end
        UNION ALL
        SELECT COUNT(*) FILTER (WHERE status IN ('verified','provisional')),
               COUNT(*) FILTER (WHERE status NOT IN ('verified','provisional')),
               COALESCE(SUM(difficulty) FILTER (WHERE status IN ('verified','provisional')),0)::bigint
        FROM shares WHERE created_at >= p_start AND created_at < full_start
        UNION ALL
        SELECT COUNT(*) FILTER (WHERE status IN ('verified','provisional')),
               COUNT(*) FILTER (WHERE status NOT IN ('verified','provisional')),
               COALESCE(SUM(difficulty) FILTER (WHERE status IN ('verified','provisional')),0)::bigint
        FROM shares WHERE created_at >= full_end AND created_at <= p_end
    ) x;
END
$window$;

CREATE OR REPLACE FUNCTION share_read_address_window(p_start BIGINT,p_end BIGINT)
RETURNS TABLE(address TEXT,share_status TEXT,share_count BIGINT,share_work BIGINT)
LANGUAGE plpgsql STABLE ROWS 128 AS $address_window$
DECLARE
    full_start BIGINT;
    full_end BIGINT;
BEGIN
    full_start := p_start / 60 * 60 + CASE WHEN p_start % 60 > 0 THEN 60 ELSE 0 END;
    full_end := p_end / 60 * 60 + CASE WHEN p_end % 60 = 59 THEN 60 ELSE 0 END;
    IF full_start >= full_end
       OR NOT COALESCE((SELECT ready FROM share_read_state WHERE singleton),FALSE) THEN
        RETURN QUERY SELECT s.miner,
            CASE WHEN s.status IN ('','verified','provisional') THEN s.status ELSE 'rejected' END,
            COUNT(*),SUM(s.difficulty)::bigint
        FROM shares s WHERE s.created_at >= p_start AND s.created_at <= p_end
        GROUP BY 1,2;
        RETURN;
    END IF;
    RETURN QUERY SELECT x.a,x.s,SUM(x.n)::bigint,SUM(x.w)::bigint
    FROM (
        SELECT m.miner AS a,m.status AS s,m.shares AS n,m.work AS w
        FROM share_read_miners m WHERE m.minute >= full_start AND m.minute < full_end
        UNION ALL
        SELECT d.miner,d.status,d.shares,d.work
        FROM share_read_deltas d WHERE d.minute >= full_start AND d.minute < full_end
        UNION ALL
        SELECT s.miner,
               CASE WHEN s.status IN ('','verified','provisional') THEN s.status ELSE 'rejected' END,
               COUNT(*),SUM(s.difficulty)::bigint
        FROM shares s WHERE s.created_at >= p_start AND s.created_at < full_start GROUP BY 1,2
        UNION ALL
        SELECT s.miner,
               CASE WHEN s.status IN ('','verified','provisional') THEN s.status ELSE 'rejected' END,
               COUNT(*),SUM(s.difficulty)::bigint
        FROM shares s WHERE s.created_at >= full_end AND s.created_at <= p_end GROUP BY 1,2
    ) x GROUP BY x.a,x.s HAVING SUM(x.n) > 0;
END
$address_window$;
