-- Exact read projection. Writers only append deltas; they never lock a shared
-- miner counter. Reads include unapplied deltas in the same MVCC snapshot.
CREATE TABLE IF NOT EXISTS miner_share_counts (
    miner TEXT PRIMARY KEY,
    accepted BIGINT NOT NULL,
    rejected BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS miner_share_count_deltas (
    id BIGSERIAL PRIMARY KEY,
    miner TEXT NOT NULL,
    accepted BIGINT NOT NULL,
    rejected BIGINT NOT NULL
) WITH (autovacuum_vacuum_threshold = 1000, autovacuum_vacuum_scale_factor = 0);
CREATE TABLE IF NOT EXISTS miner_share_count_state (
    singleton BOOLEAN PRIMARY KEY CHECK (singleton),
    ready BOOLEAN NOT NULL
);

CREATE OR REPLACE FUNCTION capture_miner_share_count_delta() RETURNS trigger
LANGUAGE plpgsql AS $function$
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        DELETE FROM miner_share_count_deltas;
        DELETE FROM miner_share_counts;
        UPDATE miner_share_count_state SET ready = TRUE WHERE singleton;
        RETURN NULL;
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.miner = NEW.miner
       AND (OLD.status IN ('verified','provisional')) = (NEW.status IN ('verified','provisional')) THEN
        RETURN NULL;
    END IF;
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        INSERT INTO miner_share_count_deltas(miner,accepted,rejected)
        VALUES (OLD.miner, -(OLD.status IN ('verified','provisional'))::int,
                -(OLD.status NOT IN ('verified','provisional'))::int);
    END IF;
    IF TG_OP IN ('UPDATE', 'INSERT') THEN
        INSERT INTO miner_share_count_deltas(miner,accepted,rejected)
        VALUES (NEW.miner, (NEW.status IN ('verified','provisional'))::int,
                (NEW.status NOT IN ('verified','provisional'))::int);
    END IF;
    RETURN NULL;
END
$function$;

DO $install$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger
                   WHERE tgrelid = 'shares'::regclass
                     AND tgname = 'shares_read_count_delta') THEN
        CREATE TRIGGER shares_read_count_delta
        AFTER INSERT OR DELETE OR UPDATE OF miner,status ON shares
        FOR EACH ROW EXECUTE FUNCTION capture_miner_share_count_delta();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger
                   WHERE tgrelid = 'shares'::regclass
                     AND tgname = 'shares_read_count_truncate') THEN
        CREATE TRIGGER shares_read_count_truncate AFTER TRUNCATE ON shares
        FOR EACH STATEMENT EXECUTE FUNCTION capture_miner_share_count_delta();
    END IF;
END
$install$;

-- Only an empty database can initialize without the online backfill script.
-- Installing the trigger above fences preexisting writers before this check.
INSERT INTO miner_share_count_state(singleton,ready)
VALUES (TRUE, NOT EXISTS (SELECT 1 FROM shares LIMIT 1))
ON CONFLICT (singleton) DO NOTHING;
