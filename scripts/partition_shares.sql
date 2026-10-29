-- Converts the plain `shares` table into weekly RANGE (created_at) partitions
-- without stopping share writes. Run it through scripts/partition_shares.sh,
-- which selects one step per invocation and retries lock timeouts:
--
--   preflight     read-only checks and the cutoff the migration will use
--   parent        new partitioned parent `shares_partitioned`, its indexes
--                 (ON ONLY, copied from shares), triggers, DEFAULT partition
--                 and the weekly partitions from the cutoff
--   unique-index  CREATE UNIQUE INDEX CONCURRENTLY on shares (id, created_at)
--   check         CHECK (created_at < cutoff) NOT VALID, then VALIDATE
--   swap          one short transaction: shares becomes shares_legacy and is
--                 attached for [MINVALUE, cutoff); shares_partitioned becomes
--                 shares and takes the index names and the id sequence
--   status        what is done so far
--   rollback      undo parent/unique-index/check (before the swap only)
--
-- The cutoff is a Monday 00:00 UTC (unix seconds), recorded on the new parent
-- by the parent step and reused by later steps. Shares stamped before it stay
-- in shares_legacy, which retention empties row by row and drops once all of
-- it is past the retention cutoff. The CHECK constraint rejects inserts at or
-- after the cutoff until the swap, so the swap must finish before then; run
-- rollback otherwise.
--
-- Every step is idempotent. With ps_dry_run=on, DDL is printed, not run.
-- With ps_rehearse=on the swap runs and is rolled back (it takes its locks).

\set ON_ERROR_STOP on
\set QUIET on
\if :{?ps_dry_run}
\else
\set ps_dry_run off
\endif
\if :{?ps_rehearse}
\else
\set ps_rehearse off
\endif
\if :{?ps_cutoff}
\else
\set ps_cutoff ''
\endif
\if :{?ps_weeks}
\else
\set ps_weeks 4
\endif
\if :{?ps_min_lead_hours}
\else
\set ps_min_lead_hours 48
\endif
\if :{?ps_step}
\else
\echo 'set ps_step (see scripts/partition_shares.sh)'
\quit
\endif

SELECT set_config('partition_shares.dry_run', :'ps_dry_run', false) AS ps_ignored,
       set_config('partition_shares.cutoff', :'ps_cutoff', false) AS ps_ignored,
       set_config('partition_shares.weeks', :'ps_weeks', false) AS ps_ignored,
       set_config('partition_shares.min_lead_hours', :'ps_min_lead_hours', false) AS ps_ignored
\gset

SELECT :'ps_step' = 'preflight' AS ps_preflight,
       :'ps_step' = 'parent' AS ps_parent,
       :'ps_step' = 'unique-index' AS ps_unique_index,
       :'ps_step' = 'check' AS ps_check,
       :'ps_step' = 'swap' AS ps_swap,
       :'ps_step' = 'status' AS ps_status,
       :'ps_step' = 'rollback' AS ps_rollback,
       :'ps_step' IN ('preflight','parent','unique-index','check','swap','status','rollback')
           AS ps_known_step
\gset
\if :ps_known_step
\else
\echo 'unknown step' :'ps_step'
\quit
\endif

-- Session-local helpers; pg_temp objects vanish when psql exits.
CREATE FUNCTION pg_temp.ps_dry_run() RETURNS boolean LANGUAGE sql AS $$
    SELECT current_setting('partition_shares.dry_run') IN ('on', 'true', '1')
$$;

CREATE FUNCTION pg_temp.ps_exec(statement text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF pg_temp.ps_dry_run() THEN
        RAISE NOTICE 'dry-run: %;', statement;
    ELSE
        RAISE NOTICE 'run: %;', statement;
        EXECUTE statement;
    END IF;
END
$$;

CREATE FUNCTION pg_temp.ps_is_partitioned() RETURNS boolean LANGUAGE sql AS $$
    SELECT COALESCE((SELECT relkind = 'p' FROM pg_class WHERE oid = to_regclass('shares')), false)
$$;

CREATE FUNCTION pg_temp.ps_recorded_cutoff() RETURNS bigint LANGUAGE sql AS $$
    SELECT substring(obj_description(to_regclass('shares_partitioned'), 'pg_class')
                     FROM 'cutoff=([0-9]+)')::bigint
$$;

-- The requested cutoff, else the recorded one, else the first Monday 00:00 UTC
-- at least ps_min_lead_hours from now.
CREATE FUNCTION pg_temp.ps_cutoff() RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    week CONSTANT bigint := 604800;
    monday CONSTANT bigint := 345600; -- 1970-01-05 00:00:00 UTC
    recorded bigint := pg_temp.ps_recorded_cutoff();
    requested text := current_setting('partition_shares.cutoff');
    lead_secs bigint := current_setting('partition_shares.min_lead_hours')::bigint * 3600;
    candidate bigint;
BEGIN
    IF requested <> '' THEN
        IF requested !~ '^[0-9]+$' THEN
            RAISE EXCEPTION 'cutoff % is not unix seconds', requested;
        END IF;
        candidate := requested::bigint;
        IF (candidate - monday) % week <> 0 THEN
            RAISE EXCEPTION 'cutoff % is not a Monday 00:00 UTC', candidate;
        END IF;
        IF recorded IS NOT NULL AND recorded <> candidate THEN
            RAISE EXCEPTION 'cutoff % differs from the recorded cutoff %', candidate, recorded;
        END IF;
        RETURN candidate;
    END IF;
    IF recorded IS NOT NULL THEN
        RETURN recorded;
    END IF;
    candidate := extract(epoch FROM now())::bigint + lead_secs;
    RETURN monday + ((candidate - monday + week - 1) / week) * week;
END
$$;

CREATE FUNCTION pg_temp.ps_ts(ts bigint) RETURNS text LANGUAGE sql AS $$
    SELECT to_char(to_timestamp(ts) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI "UTC"')
$$;

-- An index definition with its name and table stripped, for comparisons.
CREATE FUNCTION pg_temp.ps_index_shape(index_oid oid) RETURNS text LANGUAGE sql AS $$
    SELECT CASE WHEN i.indisunique THEN 'UNIQUE' ELSE '' END
           || substr(pg_get_indexdef(index_oid), strpos(pg_get_indexdef(index_oid), ' USING '))
    FROM pg_index i WHERE i.indexrelid = index_oid
$$;

-- Indexes of the plain shares table that the parent copies: all but the
-- primary key and the (id, created_at) index that replaces it.
CREATE FUNCTION pg_temp.ps_copied_indexes()
RETURNS TABLE(index_oid oid, index_name text) LANGUAGE sql AS $$
    SELECT i.indexrelid, c.relname::text
    FROM pg_index i
    JOIN pg_class c ON c.oid = i.indexrelid
    WHERE i.indrelid = to_regclass('shares')
      AND NOT i.indisprimary
      AND c.relname <> 'shares_id_created_at_key'
    ORDER BY c.relname
$$;

CREATE FUNCTION pg_temp.ps_reloptions_sql(rel regclass) RETURNS text LANGUAGE sql AS $$
    SELECT COALESCE(' WITH (' || string_agg(
               format('%s = %L', split_part(option, '=', 1),
                      substr(option, strpos(option, '=') + 1)), ', ') || ')', '')
    FROM pg_class c, unnest(c.reloptions) option
    WHERE c.oid = rel
$$;

\if :ps_preflight
\echo '== preflight (read-only) =='
SELECT current_setting('server_version') AS server_version,
       current_user,
       CASE WHEN to_regclass('shares') IS NULL THEN 'missing'
            WHEN pg_temp.ps_is_partitioned() THEN 'partitioned (already migrated)'
            ELSE 'plain' END AS shares_layout;
SELECT c.reltuples::bigint AS estimated_rows,
       pg_size_pretty(pg_table_size(c.oid)) AS heap_size,
       pg_size_pretty(pg_indexes_size(c.oid)) AS index_size,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
       pg_get_userbyid(c.relowner) AS owner,
       c.reloptions
FROM pg_class c WHERE c.oid = to_regclass('shares');
SELECT pg_temp.ps_cutoff() AS cutoff,
       pg_temp.ps_ts(pg_temp.ps_cutoff()) AS cutoff_utc,
       round((pg_temp.ps_cutoff() - extract(epoch FROM now())) / 3600.0, 1) AS hours_until_cutoff,
       pg_temp.ps_recorded_cutoff() IS NOT NULL AS cutoff_recorded,
       (SELECT min(created_at) FROM shares) AS min_created_at,
       (SELECT max(created_at) FROM shares) AS max_created_at;
SELECT c.relname AS index_name, i.indisvalid AS valid, i.indisunique AS is_unique,
       pg_size_pretty(pg_relation_size(c.oid)) AS size, pg_get_indexdef(c.oid) AS definition
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
WHERE i.indrelid = to_regclass('shares') ORDER BY c.relname;
SELECT tgname AS trigger_name, pg_get_triggerdef(oid) AS definition
FROM pg_trigger WHERE tgrelid = to_regclass('shares') AND NOT tgisinternal ORDER BY tgname;
DO $preflight$
DECLARE
    shares_oid regclass := to_regclass('shares');
    owner_oid oid;
    seq regclass;
    blockers text[] := ARRAY[]::text[];
    cutoff bigint := pg_temp.ps_cutoff();
    max_created bigint;
    item text;
BEGIN
    IF shares_oid IS NULL THEN
        RAISE EXCEPTION 'shares does not exist';
    END IF;
    IF pg_temp.ps_is_partitioned() THEN
        RAISE NOTICE 'shares is already partitioned; nothing to migrate';
        RETURN;
    END IF;
    IF current_setting('server_version_num')::int < 130000 THEN
        blockers := blockers || 'PostgreSQL 13 or newer is required'::text;
    END IF;
    SELECT relowner INTO owner_oid FROM pg_class WHERE oid = shares_oid;
    IF NOT pg_has_role(current_user, owner_oid, 'USAGE') THEN
        blockers := blockers || format('run as %s (the owner of shares) or a superuser',
                                       pg_get_userbyid(owner_oid));
    END IF;
    seq := pg_get_serial_sequence('shares', 'id')::regclass;
    IF seq IS NULL THEN
        blockers := blockers || 'shares.id has no owned sequence'::text;
    ELSIF (SELECT relowner FROM pg_class WHERE oid = seq) <> owner_oid THEN
        blockers := blockers || format('sequence %s is not owned by the owner of shares', seq);
    END IF;
    FOR item IN
        SELECT format('foreign key %s on %s references shares', conname, conrelid::regclass)
        FROM pg_constraint WHERE confrelid = shares_oid AND contype = 'f'
    LOOP
        blockers := blockers || item;
    END LOOP;
    FOR item IN
        SELECT format('index %s is invalid; drop it first', c.relname)
        FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
        WHERE i.indrelid = shares_oid AND NOT i.indisvalid
          AND c.relname <> 'shares_id_created_at_key'
    LOOP
        blockers := blockers || item;
    END LOOP;
    FOR item IN
        SELECT format('unique index %s cannot carry over to a partitioned table', index_name)
        FROM pg_temp.ps_copied_indexes() x
        JOIN pg_index i ON i.indexrelid = x.index_oid
        WHERE i.indisunique
    LOOP
        blockers := blockers || item;
    END LOOP;
    FOR item IN
        SELECT format('constraint trigger %s', tgname)
        FROM pg_trigger WHERE tgrelid = shares_oid AND tgconstraint <> 0
    LOOP
        blockers := blockers || item;
    END LOOP;
    SELECT max(created_at) INTO max_created FROM shares;
    IF max_created >= cutoff THEN
        blockers := blockers || format('shares has created_at %s at or after the cutoff %s',
                                       max_created, cutoff);
    END IF;
    IF cutoff - extract(epoch FROM now())::bigint < 6 * 3600 THEN
        blockers := blockers || format('cutoff %s is less than 6 hours away', pg_temp.ps_ts(cutoff));
    END IF;
    IF EXISTS (SELECT 1 FROM pg_temp.ps_copied_indexes()
               WHERE index_name = 'idx_shares_miner_created')
       AND NOT EXISTS (SELECT 1 FROM pg_index
                       WHERE indexrelid = to_regclass('idx_shares_miner_created')
                         AND pg_get_expr(indpred, indrelid) = 'false') THEN
        RAISE WARNING 'idx_shares_miner_created is a real index, not the empty WHERE false stub; it will be built on every new partition';
    END IF;
    IF cardinality(blockers) > 0 THEN
        RAISE EXCEPTION 'preflight failed:%', E'\n  - ' || array_to_string(blockers, E'\n  - ');
    END IF;
    RAISE NOTICE 'preflight ok: cutoff % (%), % weekly partitions from it',
        cutoff, pg_temp.ps_ts(cutoff), current_setting('partition_shares.weeks');
END
$preflight$;
\endif

\if :ps_parent
\echo '== parent =='
BEGIN;
SET LOCAL lock_timeout = '5s';
DO $parent$
DECLARE
    cutoff bigint := pg_temp.ps_cutoff();
    weeks int := current_setting('partition_shares.weeks')::int;
    shares_oid regclass := to_regclass('shares');
    schema_name text;
    owner_name text;
    reloptions_sql text;
    qualified_old text;
    qualified_new text;
    lower_bound bigint;
    partition_name text;
    copied record;
    definition text;
    prefix text;
    grant_row record;
BEGIN
    IF pg_temp.ps_is_partitioned() THEN
        RAISE NOTICE 'shares is already partitioned; nothing to do';
        RETURN;
    END IF;
    IF shares_oid IS NULL THEN
        RAISE EXCEPTION 'shares does not exist';
    END IF;
    IF (SELECT max(created_at) FROM shares) >= cutoff THEN
        RAISE EXCEPTION 'shares has rows at or after the cutoff %', cutoff;
    END IF;
    SELECT n.nspname, pg_get_userbyid(c.relowner)
    INTO schema_name, owner_name
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = shares_oid;
    qualified_old := format('%I.%I', schema_name, 'shares');
    qualified_new := format('%I.%I', schema_name, 'shares_partitioned');
    reloptions_sql := pg_temp.ps_reloptions_sql(shares_oid);

    IF to_regclass('shares_partitioned') IS NULL THEN
        PERFORM pg_temp.ps_exec(
            'CREATE TABLE shares_partitioned (LIKE shares INCLUDING DEFAULTS, '
            || 'CONSTRAINT shares_partitioned_pkey PRIMARY KEY (id, created_at)) '
            || 'PARTITION BY RANGE (created_at)');
        PERFORM pg_temp.ps_exec(format('ALTER TABLE shares_partitioned OWNER TO %I', owner_name));
        FOR grant_row IN
            SELECT CASE WHEN a.grantee = 0 THEN 'PUBLIC'
                        ELSE quote_ident(pg_get_userbyid(a.grantee)) END AS grantee,
                   string_agg(a.privilege_type, ', ') AS privileges
            FROM pg_class c, aclexplode(c.relacl) a
            WHERE c.oid = shares_oid AND a.grantee <> c.relowner
            GROUP BY a.grantee
        LOOP
            PERFORM pg_temp.ps_exec(format('GRANT %s ON shares_partitioned TO %s',
                                           grant_row.privileges, grant_row.grantee));
        END LOOP;
        -- Same definitions, ON ONLY the parent: nothing is built on shares.
        FOR copied IN SELECT * FROM pg_temp.ps_copied_indexes() LOOP
            definition := pg_get_indexdef(copied.index_oid);
            prefix := format('CREATE INDEX %s ON %s USING ', quote_ident(copied.index_name),
                             qualified_old);
            IF left(definition, length(prefix)) <> prefix THEN
                RAISE EXCEPTION 'cannot copy index definition: %', definition;
            END IF;
            PERFORM pg_temp.ps_exec(
                format('CREATE INDEX %I ON ONLY %s USING ', 'sp_' || copied.index_name,
                       qualified_new)
                || substr(definition, length(prefix) + 1));
        END LOOP;
        -- Row triggers clone onto each partition; statement triggers fire on
        -- the parent once shares points at it.
        FOR copied IN
            SELECT tgname, pg_get_triggerdef(oid) AS definition
            FROM pg_trigger WHERE tgrelid = shares_oid AND NOT tgisinternal ORDER BY tgname
        LOOP
            prefix := format(' ON %s ', qualified_old);
            IF strpos(copied.definition, prefix) = 0 THEN
                RAISE EXCEPTION 'cannot copy trigger definition: %', copied.definition;
            END IF;
            PERFORM pg_temp.ps_exec(replace(copied.definition, prefix,
                                            format(' ON %s ', qualified_new)));
        END LOOP;
        PERFORM pg_temp.ps_exec('CREATE TABLE shares_default PARTITION OF shares_partitioned DEFAULT');
        PERFORM pg_temp.ps_exec(format('ALTER TABLE shares_default OWNER TO %I', owner_name));
        PERFORM pg_temp.ps_exec(format('COMMENT ON TABLE shares_partitioned IS %L',
            format('scripts/partition_shares.sh cutoff=%s (%s)', cutoff, pg_temp.ps_ts(cutoff))));
    ELSIF pg_temp.ps_recorded_cutoff() IS NULL THEN
        RAISE EXCEPTION 'shares_partitioned exists without a recorded cutoff; run rollback first';
    END IF;

    FOR week IN 0 .. weeks - 1 LOOP
        lower_bound := cutoff + week * 604800;
        partition_name := 'shares_p' || to_char(to_timestamp(lower_bound) AT TIME ZONE 'UTC', 'YYYYMMDD');
        CONTINUE WHEN to_regclass(partition_name) IS NOT NULL;
        PERFORM pg_temp.ps_exec(format(
            'CREATE TABLE %I PARTITION OF shares_partitioned FOR VALUES FROM (%s) TO (%s)%s',
            partition_name, lower_bound, lower_bound + 604800, reloptions_sql));
        PERFORM pg_temp.ps_exec(format('ALTER TABLE %I OWNER TO %I', partition_name, owner_name));
    END LOOP;
END
$parent$;
COMMIT;
\endif

\if :ps_unique_index
\echo '== unique-index =='
SELECT pg_temp.ps_is_partitioned() AS ps_done,
       EXISTS (SELECT 1 FROM pg_index
               WHERE indexrelid = to_regclass('shares_id_created_at_key')
                 AND NOT indisvalid) AS ps_invalid
\gset
\if :ps_done
\echo 'shares is already partitioned; nothing to do'
\else
-- Builds without blocking share writes; waits out older transactions.
SET lock_timeout = 0;
SET statement_timeout = 0;
SET maintenance_work_mem = '1GB';
\if :ps_invalid
\if :ps_dry_run
\echo 'dry-run: DROP INDEX CONCURRENTLY shares_id_created_at_key;'
\else
\echo 'run: DROP INDEX CONCURRENTLY shares_id_created_at_key;'
DROP INDEX CONCURRENTLY shares_id_created_at_key;
\endif
\endif
\if :ps_dry_run
\echo 'dry-run: CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS shares_id_created_at_key ON shares (id, created_at);'
\else
\echo 'run: CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS shares_id_created_at_key ON shares (id, created_at);'
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS shares_id_created_at_key ON shares (id, created_at);
SELECT indexrelid::regclass AS index_name, indisvalid AS valid,
       pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_index WHERE indexrelid = to_regclass('shares_id_created_at_key');
\endif
\endif
\endif

\if :ps_check
\echo '== check =='
SELECT pg_temp.ps_is_partitioned() AS ps_done \gset
\if :ps_done
\echo 'shares is already partitioned; nothing to do'
\else
BEGIN;
SET LOCAL lock_timeout = '2s';
DO $check$
DECLARE
    cutoff bigint := pg_temp.ps_recorded_cutoff();
    existing text;
BEGIN
    IF cutoff IS NULL THEN
        RAISE EXCEPTION 'run the parent step first';
    END IF;
    IF cutoff - extract(epoch FROM now())::bigint < 2 * 3600 THEN
        RAISE EXCEPTION 'cutoff % is less than 2 hours away; run rollback and start again',
            pg_temp.ps_ts(cutoff);
    END IF;
    SELECT pg_get_constraintdef(oid) INTO existing
    FROM pg_constraint
    WHERE conrelid = 'shares'::regclass AND conname = 'shares_created_at_before_partitions';
    IF existing IS NOT NULL THEN
        IF existing <> format('CHECK ((created_at < %s))', cutoff) THEN
            RAISE EXCEPTION 'shares_created_at_before_partitions is % but the cutoff is %',
                existing, cutoff;
        END IF;
        RETURN;
    END IF;
    IF (SELECT max(created_at) FROM shares) >= cutoff THEN
        RAISE EXCEPTION 'shares has rows at or after the cutoff %', cutoff;
    END IF;
    -- NOT VALID needs only a moment of ACCESS EXCLUSIVE; it applies to new
    -- rows at once.
    PERFORM pg_temp.ps_exec(format(
        'ALTER TABLE shares ADD CONSTRAINT shares_created_at_before_partitions '
        'CHECK (created_at < %s) NOT VALID', cutoff));
END
$check$;
COMMIT;
SELECT EXISTS (SELECT 1 FROM pg_constraint
               WHERE conrelid = 'shares'::regclass
                 AND conname = 'shares_created_at_before_partitions'
                 AND NOT convalidated) AS ps_needs_validate
\gset
\if :ps_needs_validate
-- Scans the table under SHARE UPDATE EXCLUSIVE; share writes continue.
SET statement_timeout = 0;
\if :ps_dry_run
\echo 'dry-run: ALTER TABLE shares VALIDATE CONSTRAINT shares_created_at_before_partitions;'
\else
\echo 'run: ALTER TABLE shares VALIDATE CONSTRAINT shares_created_at_before_partitions;'
ALTER TABLE shares VALIDATE CONSTRAINT shares_created_at_before_partitions;
\endif
\elif :ps_dry_run
\echo 'dry-run: ALTER TABLE shares VALIDATE CONSTRAINT shares_created_at_before_partitions;'
\endif
\endif
\endif

\if :ps_swap
\echo '== swap =='
BEGIN;
-- A lock wait stalls share writes queued behind it, so give up fast; the
-- wrapper retries. Nothing here scans or builds: an unexpected index build or
-- partition scan hits the statement timeout and rolls everything back.
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';
DO $swap$
DECLARE
    cutoff bigint := pg_temp.ps_recorded_cutoff();
    legacy_pkey text;
    seq text;
    copied_names text[];
    index_name text;
    trigger_name text;
    problems text[] := ARRAY[]::text[];
    item text;
BEGIN
    IF pg_temp.ps_is_partitioned() THEN
        IF EXISTS (SELECT 1 FROM pg_inherits
                   WHERE inhparent = 'shares'::regclass
                     AND inhrelid = to_regclass('shares_legacy')) THEN
            RAISE NOTICE 'already swapped: shares_legacy is a partition of shares';
        ELSE
            RAISE NOTICE 'shares is partitioned without a legacy partition; nothing to swap';
        END IF;
        RETURN;
    END IF;

    -- Preconditions, all read-only.
    IF cutoff IS NULL THEN
        problems := problems || 'run the parent step first'::text;
    ELSIF cutoff <= extract(epoch FROM now())::bigint THEN
        problems := problems || format('the cutoff %s has passed; run rollback', pg_temp.ps_ts(cutoff));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_index
                   WHERE indexrelid = to_regclass('shares_id_created_at_key')
                     AND indrelid = 'shares'::regclass AND indisvalid AND indisunique
                     AND pg_temp.ps_index_shape(indexrelid) = 'UNIQUE USING btree (id, created_at)') THEN
        problems := problems || 'run the unique-index step first'::text;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'shares'::regclass
                     AND conname = 'shares_created_at_before_partitions' AND convalidated
                     AND pg_get_constraintdef(oid) = format('CHECK ((created_at < %s))', cutoff)) THEN
        problems := problems || 'run the check step first'::text;
    END IF;
    FOR item IN
        SELECT format('index %s has no matching sp_%s on shares_partitioned', x.index_name, x.index_name)
        FROM pg_temp.ps_copied_indexes() x
        WHERE pg_temp.ps_index_shape(x.index_oid) IS DISTINCT FROM
              pg_temp.ps_index_shape(to_regclass('sp_' || x.index_name))
        UNION ALL
        SELECT format('shares_partitioned index %s has no source index on shares', c.relname)
        FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
        WHERE i.indrelid = to_regclass('shares_partitioned') AND NOT i.indisprimary
          AND NOT EXISTS (SELECT 1 FROM pg_temp.ps_copied_indexes() x
                          WHERE 'sp_' || x.index_name = c.relname)
    LOOP
        problems := problems || item;
    END LOOP;
    IF cardinality(problems) > 0 THEN
        RAISE EXCEPTION 'swap preconditions failed:%', E'\n  - ' || array_to_string(problems, E'\n  - ');
    END IF;

    SELECT conname INTO legacy_pkey FROM pg_constraint
    WHERE conrelid = 'shares'::regclass AND contype = 'p';
    seq := pg_get_serial_sequence('shares', 'id');
    copied_names := ARRAY(SELECT x.index_name FROM pg_temp.ps_copied_indexes() x);

    PERFORM pg_temp.ps_exec('LOCK TABLE shares, shares_partitioned IN ACCESS EXCLUSIVE MODE');
    -- The parent carries copies of these; its row triggers clone onto the
    -- legacy partition at ATTACH.
    FOR trigger_name IN
        SELECT tgname FROM pg_trigger
        WHERE tgrelid = 'shares'::regclass AND NOT tgisinternal ORDER BY tgname
    LOOP
        PERFORM pg_temp.ps_exec(format('DROP TRIGGER %I ON shares', trigger_name));
    END LOOP;
    PERFORM pg_temp.ps_exec('ALTER TABLE shares RENAME TO shares_legacy');
    IF legacy_pkey IS NOT NULL THEN
        PERFORM pg_temp.ps_exec(format('ALTER TABLE shares_legacy DROP CONSTRAINT %I', legacy_pkey));
    END IF;
    PERFORM pg_temp.ps_exec('ALTER TABLE shares_legacy ADD CONSTRAINT shares_legacy_pkey '
                            'PRIMARY KEY USING INDEX shares_id_created_at_key');
    -- The parent takes the index names, so old binaries' CREATE INDEX IF NOT
    -- EXISTS stays a no-op (idx_shares_miner_created included).
    FOREACH index_name IN ARRAY copied_names LOOP
        PERFORM pg_temp.ps_exec(format('ALTER INDEX %I RENAME TO %I', index_name,
                                       left('shares_legacy_' || index_name, 63)));
    END LOOP;
    PERFORM pg_temp.ps_exec('ALTER TABLE shares_partitioned RENAME TO shares');
    PERFORM pg_temp.ps_exec(format('ALTER TABLE shares RENAME CONSTRAINT shares_partitioned_pkey TO %I',
                                   COALESCE(legacy_pkey, 'shares_pkey')));
    FOREACH index_name IN ARRAY copied_names LOOP
        PERFORM pg_temp.ps_exec(format('ALTER INDEX %I RENAME TO %I', 'sp_' || index_name,
                                       index_name));
    END LOOP;
    -- Keep the sequence when shares_legacy is eventually dropped.
    PERFORM pg_temp.ps_exec(format('ALTER SEQUENCE %s OWNED BY shares.id', seq));
    -- The validated CHECK proves the bound, so ATTACH skips the scan; matching
    -- indexes attach to the parent's instead of being built.
    PERFORM pg_temp.ps_exec(format(
        'ALTER TABLE shares ATTACH PARTITION shares_legacy FOR VALUES FROM (MINVALUE) TO (%s)', cutoff));
    PERFORM pg_temp.ps_exec(format('COMMENT ON TABLE shares IS %L',
        format('partitioned by scripts/partition_shares.sh; shares_legacy holds created_at < %s (%s)',
               cutoff, pg_temp.ps_ts(cutoff))));

    IF pg_temp.ps_dry_run() THEN
        RETURN;
    END IF;
    -- Every parent index has a valid child on every partition.
    FOR item IN
        SELECT format('index %s: %s of %s partitions attached, valid=%s', p.relname,
                      (SELECT count(*) FROM pg_inherits ih WHERE ih.inhparent = p.oid),
                      (SELECT count(*) FROM pg_inherits ih WHERE ih.inhparent = 'shares'::regclass),
                      pi.indisvalid)
        FROM pg_index pi JOIN pg_class p ON p.oid = pi.indexrelid
        WHERE pi.indrelid = 'shares'::regclass
          AND (NOT pi.indisvalid
               OR (SELECT count(*) FROM pg_inherits ih WHERE ih.inhparent = p.oid)
                  <> (SELECT count(*) FROM pg_inherits ih WHERE ih.inhparent = 'shares'::regclass))
    LOOP
        problems := problems || item;
    END LOOP;
    IF EXISTS (SELECT 1 FROM pg_trigger
               WHERE tgrelid = 'shares_legacy'::regclass AND NOT tgisinternal AND tgparentid = 0) THEN
        problems := problems || 'shares_legacy kept triggers of its own'::text;
    END IF;
    IF cardinality(problems) > 0 THEN
        RAISE EXCEPTION 'swap verification failed:%', E'\n  - ' || array_to_string(problems, E'\n  - ');
    END IF;
    RAISE NOTICE 'swap ok: shares_legacy holds created_at < % (%)', cutoff, pg_temp.ps_ts(cutoff);
END
$swap$;
\if :ps_rehearse
\echo 'rehearsal: rolling back'
ROLLBACK;
\else
COMMIT;
\if :ps_dry_run
\else
-- Autovacuum never analyzes a partitioned parent; refresh its statistics.
SET statement_timeout = 0;
ANALYZE shares;
\endif
\endif
\endif

\if :ps_status
\echo '== status =='
SELECT CASE WHEN to_regclass('shares') IS NULL THEN 'missing'
            WHEN pg_temp.ps_is_partitioned() THEN 'partitioned'
            ELSE 'plain' END AS shares_layout,
       to_regclass('shares_partitioned') IS NOT NULL AS parent_created,
       pg_temp.ps_recorded_cutoff() AS recorded_cutoff,
       pg_temp.ps_ts(pg_temp.ps_recorded_cutoff()) AS recorded_cutoff_utc,
       (SELECT indisvalid FROM pg_index
        WHERE indexrelid = to_regclass('shares_id_created_at_key')) AS unique_index_valid,
       (SELECT convalidated FROM pg_constraint
        WHERE conrelid = to_regclass('shares')
          AND conname = 'shares_created_at_before_partitions') AS check_validated;
SELECT c.relname AS partition_name, pg_get_expr(c.relpartbound, c.oid) AS bound,
       c.reltuples::bigint AS estimated_rows,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
WHERE i.inhparent IN (SELECT oid FROM pg_class
                      WHERE oid IN (to_regclass('shares'), to_regclass('shares_partitioned'))
                        AND relkind = 'p')
ORDER BY c.relname;
\endif

\if :ps_rollback
\echo '== rollback =='
SELECT pg_temp.ps_is_partitioned() AS ps_done,
       EXISTS (SELECT 1 FROM pg_index
               WHERE indexrelid = to_regclass('shares_id_created_at_key')) AS ps_has_unique
\gset
\if :ps_done
\echo 'shares is already partitioned. Rollback only undoes the steps before the swap.'
\echo 'After the swap, older pool binaries keep working against the partitioned table.'
\quit
\endif
BEGIN;
SET LOCAL lock_timeout = '2s';
DO $rollback$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint
               WHERE conrelid = 'shares'::regclass
                 AND conname = 'shares_created_at_before_partitions') THEN
        PERFORM pg_temp.ps_exec('ALTER TABLE shares DROP CONSTRAINT shares_created_at_before_partitions');
    END IF;
    IF to_regclass('shares_partitioned') IS NOT NULL THEN
        -- Drops its partitions too; the id sequence stays owned by shares.
        PERFORM pg_temp.ps_exec('DROP TABLE shares_partitioned');
    END IF;
END
$rollback$;
COMMIT;
\if :ps_has_unique
SET lock_timeout = 0;
SET statement_timeout = 0;
\if :ps_dry_run
\echo 'dry-run: DROP INDEX CONCURRENTLY shares_id_created_at_key;'
\else
\echo 'run: DROP INDEX CONCURRENTLY shares_id_created_at_key;'
DROP INDEX CONCURRENTLY shares_id_created_at_key;
\endif
\endif
\endif
