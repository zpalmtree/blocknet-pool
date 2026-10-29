//! Weekly range partitions of `shares`.
//!
//! `shares` is partitioned by RANGE (created_at), in unix seconds, into weeks
//! that start Monday 00:00 UTC (`shares_pYYYYMMDD`), plus a DEFAULT partition
//! (`shares_default`) that catches any timestamp no weekly partition covers.
//! Databases created before partitioning keep a plain table until
//! `scripts/partition_shares.sh` converts them; the conversion attaches the old
//! table as `shares_legacy` for [MINVALUE, first week). The startup schema
//! never converts a table in either direction, and everything here is a no-op
//! on a plain table.
//!
//! Retention drops a weekly partition once all of it is past the cutoff, so a
//! partition straddling the cutoff keeps its expired days (up to a week) until
//! then. Rows outside weekly partitions (the legacy and DEFAULT partitions, or
//! the whole table before conversion) still expire row by row.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use postgres::error::SqlState;
use postgres::{Client, GenericClient};

pub(crate) const WEEK_SECS: i64 = 7 * 24 * 60 * 60;
/// 1970-01-05 00:00:00 UTC, the first Monday after the unix epoch.
const WEEK_EPOCH: i64 = 4 * 24 * 60 * 60;
/// Weekly partitions kept ready beyond the current one.
pub(crate) const WEEKS_AHEAD: i64 = 3;
pub(crate) const DEFAULT_PARTITION: &str = "shares_default";
/// Partition DDL takes ACCESS EXCLUSIVE on `shares`. While it waits for the
/// lock every share write queues behind it, so give up quickly and retry.
pub(crate) const DDL_LOCK_TIMEOUT: &str = "2s";
pub(crate) const DDL_LOCK_ATTEMPTS: u32 = 3;
pub(crate) const DDL_LOCK_RETRY_DELAY: Duration = Duration::from_millis(500);
/// The DEFAULT partition row count is sampled up to this many rows.
const DEFAULT_ROWS_SAMPLE: i64 = 10_000;

/// Start of the partition week containing `ts`.
pub(crate) fn week_start(ts: i64) -> i64 {
    WEEK_EPOCH + (ts - WEEK_EPOCH).div_euclid(WEEK_SECS) * WEEK_SECS
}

pub(crate) fn weekly_partition_name(week_start: i64) -> String {
    let (year, month, day) = civil_from_days(week_start.div_euclid(24 * 60 * 60));
    format!("shares_p{year:04}{month:02}{day:02}")
}

/// Proleptic Gregorian (year, month, day) for days since 1970-01-01.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

pub(crate) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SharePartition {
    pub name: String,
    /// Inclusive lower bound; `None` is MINVALUE.
    pub lower: Option<i64>,
    /// Exclusive upper bound; `None` is MAXVALUE.
    pub upper: Option<i64>,
    pub is_default: bool,
}

impl SharePartition {
    /// A weekly partition, dropped whole once expired. The DEFAULT and legacy
    /// partitions are unbounded on at least one side and expire by row.
    pub(crate) fn is_bounded(&self) -> bool {
        !self.is_default && self.lower.is_some() && self.upper.is_some()
    }

    fn overlaps(&self, lower: i64, upper: i64) -> bool {
        !self.is_default
            && self.lower.is_none_or(|start| start < upper)
            && self.upper.is_none_or(|end| lower < end)
    }
}

/// The partitions of `shares`, or `None` while it is a plain table.
pub(crate) fn list_partitions<C: GenericClient>(
    client: &mut C,
) -> Result<Option<Vec<SharePartition>>> {
    let partitioned = client
        .query_one(
            "SELECT COALESCE((SELECT relkind = 'p' FROM pg_class
                              WHERE oid = to_regclass('shares')), FALSE)",
            &[],
        )
        .context("inspect shares table layout")?
        .get::<_, bool>(0);
    if !partitioned {
        return Ok(None);
    }
    let rows = client
        .query(
            "SELECT c.relname::text, pg_get_expr(c.relpartbound, c.oid)
             FROM pg_inherits i
             JOIN pg_class c ON c.oid = i.inhrelid
             WHERE i.inhparent = 'shares'::regclass
             ORDER BY c.relname",
            &[],
        )
        .context("list shares partitions")?;
    let mut partitions = rows
        .iter()
        .map(|row| {
            let name = row.get::<_, String>(0);
            let bound = row.get::<_, String>(1);
            let (lower, upper, is_default) = parse_partition_bound(&bound)
                .with_context(|| format!("parse bound of share partition {name}: {bound}"))?;
            Ok(SharePartition {
                name,
                lower,
                upper,
                is_default,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    partitions.sort_by_key(|partition| (partition.is_default, partition.lower));
    Ok(Some(partitions))
}

/// Parses `pg_get_expr(relpartbound)` for a single-column range partition:
/// `DEFAULT` or `FOR VALUES FROM ('1') TO ('2')`, either side possibly
/// MINVALUE/MAXVALUE.
fn parse_partition_bound(bound: &str) -> Result<(Option<i64>, Option<i64>, bool)> {
    let bound = bound.trim();
    if bound.eq_ignore_ascii_case("DEFAULT") {
        return Ok((None, None, true));
    }
    let rest = bound
        .strip_prefix("FOR VALUES FROM (")
        .ok_or_else(|| anyhow!("not a range bound"))?;
    let (lower, rest) = rest
        .split_once(") TO (")
        .ok_or_else(|| anyhow!("missing upper bound"))?;
    let upper = rest
        .strip_suffix(')')
        .ok_or_else(|| anyhow!("unterminated upper bound"))?;
    let value = |raw: &str| -> Result<Option<i64>> {
        let raw = raw.trim();
        if raw.eq_ignore_ascii_case("MINVALUE") || raw.eq_ignore_ascii_case("MAXVALUE") {
            return Ok(None);
        }
        Ok(Some(raw.trim_matches('\'').parse::<i64>()?))
    };
    Ok((value(lower)?, value(upper)?, false))
}

/// Ranges of `created_at` below `cutoff` that no weekly partition covers.
/// Their rows expire by DELETE; before conversion this is everything below
/// the cutoff. `weekly` must be sorted by lower bound.
pub(crate) fn row_retention_ranges(weekly: &[SharePartition], cutoff: i64) -> Vec<(i64, i64)> {
    let mut ranges = Vec::new();
    let mut start = i64::MIN;
    for partition in weekly {
        let (Some(lower), Some(upper)) = (partition.lower, partition.upper) else {
            continue;
        };
        if lower >= cutoff {
            break;
        }
        if start < lower {
            ranges.push((start, lower));
        }
        start = start.max(upper);
    }
    if start < cutoff {
        ranges.push((start, cutoff));
    }
    ranges
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SharePartitionMaintenance {
    pub partitioned: bool,
    pub created: Vec<String>,
    /// Rows in the DEFAULT partition, sampled up to a cap. Anything here means
    /// a share timestamp fell outside every weekly partition.
    pub default_rows: u64,
}

/// Ensures the DEFAULT partition and the weekly partitions from the week of
/// `now` through [`WEEKS_AHEAD`] weeks later exist. New weekly partitions copy
/// the storage options (autovacuum tuning) of the newest existing one.
pub(crate) fn maintain(client: &mut Client, now: i64) -> Result<SharePartitionMaintenance> {
    let mut report = SharePartitionMaintenance::default();
    let Some(partitions) = list_partitions(client)? else {
        return Ok(report);
    };
    report.partitioned = true;
    if !partitions.iter().any(|partition| partition.is_default) {
        run_partition_ddl(
            client,
            &format!(
                "CREATE TABLE IF NOT EXISTS {} PARTITION OF shares DEFAULT",
                quote_ident(DEFAULT_PARTITION)
            ),
        )
        .context("create default share partition")?;
        report.created.push(DEFAULT_PARTITION.to_string());
    }
    let storage = match partitions
        .iter()
        .filter(|partition| !partition.is_default)
        .max_by_key(|partition| partition.lower)
    {
        Some(newest) => storage_options_clause(client, &newest.name)?,
        None => String::new(),
    };
    let first = week_start(now);
    for week in 0..=WEEKS_AHEAD {
        let lower = first + week * WEEK_SECS;
        let upper = lower + WEEK_SECS;
        if partitions
            .iter()
            .any(|partition| partition.overlaps(lower, upper))
        {
            continue;
        }
        let name = weekly_partition_name(lower);
        run_partition_ddl(
            client,
            &format!(
                "CREATE TABLE IF NOT EXISTS {} PARTITION OF shares FOR VALUES FROM ({lower}) TO ({upper}){storage}",
                quote_ident(&name)
            ),
        )
        .with_context(|| format!("create share partition {name}"))?;
        report.created.push(name);
    }
    report.default_rows = client
        .query_one(
            &format!(
                "SELECT COUNT(*) FROM (SELECT 1 FROM {} LIMIT {DEFAULT_ROWS_SAMPLE}) sample",
                quote_ident(DEFAULT_PARTITION)
            ),
            &[],
        )
        .context("count default share partition rows")?
        .get::<_, i64>(0)
        .max(0) as u64;
    Ok(report)
}

/// ` WITH (...)` repeating a table's reloptions, or empty without any.
fn storage_options_clause(client: &mut Client, table: &str) -> Result<String> {
    let options = client
        .query(
            "SELECT split_part(option, '=', 1), substr(option, strpos(option, '=') + 1)
             FROM pg_class c, unnest(c.reloptions) option
             WHERE c.oid = to_regclass($1)",
            &[&quote_ident(table)],
        )
        .with_context(|| format!("read storage options of {table}"))?
        .iter()
        .map(|row| {
            let name = row.get::<_, String>(0);
            let value = row.get::<_, String>(1);
            format!("{} = '{}'", quote_ident(&name), value.replace('\'', "''"))
        })
        .collect::<Vec<_>>();
    if options.is_empty() {
        return Ok(String::new());
    }
    Ok(format!(" WITH ({})", options.join(", ")))
}

pub(crate) fn is_lock_timeout(err: &postgres::Error) -> bool {
    err.code() == Some(&SqlState::LOCK_NOT_AVAILABLE)
}

/// Runs one partition DDL statement in its own transaction under
/// [`DDL_LOCK_TIMEOUT`], retrying lock timeouts.
fn run_partition_ddl(client: &mut Client, sql: &str) -> Result<()> {
    let mut attempt = 1;
    loop {
        let result = client.transaction().and_then(|mut tx| {
            tx.batch_execute(&format!("SET LOCAL lock_timeout = '{DDL_LOCK_TIMEOUT}'"))?;
            tx.batch_execute(sql)?;
            tx.commit()
        });
        match result {
            Ok(()) => return Ok(()),
            Err(err) if is_lock_timeout(&err) && attempt < DDL_LOCK_ATTEMPTS => {
                attempt += 1;
                std::thread::sleep(DDL_LOCK_RETRY_DELAY);
            }
            Err(err) => return Err(err.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn weekly(lower: i64) -> SharePartition {
        SharePartition {
            name: weekly_partition_name(lower),
            lower: Some(lower),
            upper: Some(lower + WEEK_SECS),
            is_default: false,
        }
    }

    #[test]
    fn weeks_start_on_monday_utc() {
        // 2026-10-29 12:00:00 UTC is a Thursday.
        let thursday = 1_793_275_200;
        let start = week_start(thursday);
        assert_eq!(weekly_partition_name(start), "shares_p20261026");
        assert_eq!(week_start(start), start);
        assert_eq!(week_start(start - 1), start - WEEK_SECS);
        assert_eq!(weekly_partition_name(start + WEEK_SECS), "shares_p20261102");
        assert_eq!(weekly_partition_name(week_start(0)), "shares_p19691229");
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29));
    }

    #[test]
    fn parses_range_and_default_bounds() {
        assert_eq!(
            parse_partition_bound("FOR VALUES FROM ('1761523200') TO ('1762128000')").unwrap(),
            (Some(1_761_523_200), Some(1_762_128_000), false)
        );
        assert_eq!(
            parse_partition_bound("FOR VALUES FROM (MINVALUE) TO ('5')").unwrap(),
            (None, Some(5), false)
        );
        assert_eq!(
            parse_partition_bound("DEFAULT").unwrap(),
            (None, None, true)
        );
        assert!(parse_partition_bound("FOR VALUES IN ('a')").is_err());
    }

    #[test]
    fn row_retention_skips_weekly_partitions() {
        let week = week_start(1_793_275_200);
        // Plain table: everything below the cutoff expires by row.
        assert_eq!(row_retention_ranges(&[], 100), vec![(i64::MIN, 100)]);
        let partitions = [
            weekly(week),
            weekly(week + WEEK_SECS),
            weekly(week + 3 * WEEK_SECS),
        ];
        // A cutoff inside the second week leaves both weeks to partition drops.
        assert_eq!(
            row_retention_ranges(&partitions, week + WEEK_SECS + 3),
            vec![(i64::MIN, week)]
        );
        // Gaps and anything past the last partition are DEFAULT rows.
        assert_eq!(
            row_retention_ranges(&partitions, week + 5 * WEEK_SECS),
            vec![
                (i64::MIN, week),
                (week + 2 * WEEK_SECS, week + 3 * WEEK_SECS),
                (week + 4 * WEEK_SECS, week + 5 * WEEK_SECS),
            ]
        );
    }
}
